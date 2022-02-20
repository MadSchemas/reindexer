#include "coroclientconnection.h"
#include <errno.h>
#include <snappy.h>
#include <functional>
#include "core/rdxcontext.h"
#include "reindexer_version.h"
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

constexpr size_t kMaxRecycledChuncks = 1500;
constexpr size_t kMaxChunckSizeToRecycle = 2048;
constexpr size_t kMaxParallelRPCCalls = 512;
constexpr auto kCoroSleepGranularity = std::chrono::milliseconds(150);
constexpr auto kDeadlineCheckInterval = std::chrono::milliseconds(100);
constexpr auto kKeepAliveInterval = std::chrono::seconds(30);
constexpr size_t kReadBufReserveSize = 0x1000;
constexpr size_t kWrChannelSize = 20;
constexpr size_t kCntToSendNow = 30;
constexpr size_t kDataToSendNow = 2048;

CoroClientConnection::CoroClientConnection()
	: rpcCalls_(kMaxParallelRPCCalls), wrCh_(kWrChannelSize), seqNums_(kMaxParallelRPCCalls), conn_(-1, kReadBufReserveSize, false) {
	recycledChuncks_.reserve(kMaxRecycledChuncks);
	errSyncCh_.close();
	seqNums_.close();
}

CoroClientConnection::~CoroClientConnection() { Stop(); }

void CoroClientConnection::Start(ev::dynamic_loop &loop, ConnectData connectData) {
	if (!isRunning_) {
		// Don't allow to call Start, while error handling is in progress
		errSyncCh_.pop();

		if (loop_ != &loop) {
			if (loop_) {
				conn_.detach();
			}
			conn_.attach(loop);
			loop_ = &loop;
		}
		conn_.set_connect_timeout(connectData.opts.loginTimeout);

		if (!seqNums_.opened()) {
			seqNums_.reopen();
			loop_->spawn(wg_, [this] {
				for (size_t i = 1; i < seqNums_.capacity(); ++i) {
					// seq num == 0 is reserved for login
					seqNums_.push(i);
				}
			});
		}

		connectData_ = std::move(connectData);
		if (!wrCh_.opened()) {
			wrCh_.reopen();
		}

		loop_->spawn(wg_, [this] { writerRoutine(); });
		loop_->spawn(wg_, [this] { deadlineRoutine(); });
		loop_->spawn(wg_, [this] { pingerRoutine(); });

		isRunning_ = true;
	}
}

void CoroClientConnection::Stop() {
	if (isRunning_) {
		errSyncCh_.pop();
		errSyncCh_.reopen();

		terminate_ = true;
		wrCh_.close();
		conn_.close_conn(k_sock_closed_err);
		const Error err(errNetwork, "Connection closed");
		// Cancel all the system requests
		for (auto &c : rpcCalls_) {
			if (c.used && c.rspCh.opened() && !c.rspCh.full() && c.system) {
				c.rspCh.push(err);
			}
		}
		wg_.wait();
		readWg_.wait();
		terminate_ = false;
		isRunning_ = false;
		handleFatalErrorImpl(err);
	}
}

Error CoroClientConnection::Status(bool forceCheck, milliseconds netTimeout, milliseconds execTimeout, const IRdxCancelContext *ctx) {
	if (!RequiresStatusCheck() && !forceCheck) {
		return errOK;
	}
	return call({kCmdPing, netTimeout, execTimeout, lsn_t(), -1, ShardingKeyType::NotSetShard, ctx, false}, {}).Status();
}

CoroRPCAnswer CoroClientConnection::call(const CommandParams &opts, const Args &args) {
	if (opts.cancelCtx) {
		switch (opts.cancelCtx->GetCancelType()) {
			case CancelType::Explicit:
				return Error(errCanceled, "Canceled by context");
			case CancelType::Timeout:
				return Error(errTimeout, "Canceled by timeout");
			default:
				break;
		}
	}
	if (terminate_ || !isRunning_) {
		return Error(errLogic, "Client is not running");
	}

	auto deadline = opts.netTimeout.count() ? (Now() + opts.netTimeout + kDeadlineCheckInterval) : TimePointT();
	auto seqp = seqNums_.pop();
	if (!seqp.second) {
		CoroRPCAnswer(Error(errLogic, "Unable to get seq num"));
	}

	// Don't allow to add new requests, while error handling is in progress
	errSyncCh_.pop();

	uint32_t seq = seqp.first;
	auto &call = rpcCalls_[seq % rpcCalls_.size()];
	call.seq = seq;
	call.used = true;
	call.deadline = deadline;
	call.cancelCtx = opts.cancelCtx;
	call.system = (opts.cmd == kCmdPing || opts.cmd == kCmdLogin);
	CoroRPCAnswer ans;
	try {
		wrCh_.push(packRPC(
			opts.cmd, seq, args,
			Args{Arg{int64_t(opts.execTimeout.count())}, Arg{int64_t(opts.lsn)}, Arg{int64_t(opts.serverId)},
				 Arg{opts.shardingParallelExecution ? int64_t{opts.shardId} | kShardingParallelExecutionBit : int64_t{opts.shardId}}},
			opts.requiredLoginTs));
		auto ansp = call.rspCh.pop();
		if (ansp.second) {
			ans = std::move(ansp.first);
		} else {
			ans = CoroRPCAnswer(Error(errLogic, "Response channel is closed"));
		}
	} catch (...) {
		ans = CoroRPCAnswer(Error(errNetwork, "Writing channel is closed"));
	}

	call.used = false;
	seqNums_.push(seq + seqNums_.capacity());
	return ans;
}

CoroClientConnection::MarkedChunk CoroClientConnection::packRPC(CmdCode cmd, uint32_t seq, const Args &args, const Args &ctxArgs,
																std::optional<TimePointT> requiredLoginTs) {
	CProtoHeader hdr;
	hdr.len = 0;
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	hdr.compressed = enableSnappy_;
	hdr.cmd = cmd;
	hdr.seq = seq;

	chunk ch = getChunk();
	WrSerializer ser(std::move(ch));

	ser.Write(std::string_view(reinterpret_cast<char *>(&hdr), sizeof(hdr)));
	args.Pack(ser);
	ctxArgs.Pack(ser);
	if (hdr.compressed) {
		auto data = ser.Slice().substr(sizeof(hdr));
		std::string compressed;
		snappy::Compress(data.data(), data.length(), &compressed);
		ser.Reset(sizeof(hdr));
		ser.Write(compressed);
	}
	assert(ser.Len() < size_t(std::numeric_limits<int32_t>::max()));
	reinterpret_cast<CProtoHeader *>(ser.Buf())->len = ser.Len() - sizeof(hdr);

	return {seq, requiredLoginTs, ser.DetachChunk()};
}

void CoroClientConnection::appendChunck(std::vector<char> &buf, chunk &&ch) {
	auto oldBufSize = buf.size();
	buf.resize(buf.size() + ch.size());
	memcpy(buf.data() + oldBufSize, ch.data(), ch.size());
	recycleChunk(std::move(ch));
}

Error CoroClientConnection::login(std::vector<char> &buf) {
	assert(conn_.state() != manual_connection::conn_state::connecting);
	if (conn_.state() == manual_connection::conn_state::init) {
		readWg_.wait();
		string port = connectData_.uri.port().length() ? connectData_.uri.port() : string("6534");
		int ret = conn_.async_connect(connectData_.uri.hostname() + ":" + port);
		if (ret < 0) {
			// unable to connect
			return Error(errNetwork, "Connect error");
		}

		string dbName = connectData_.uri.path();
		string userName = connectData_.uri.username();
		string password = connectData_.uri.password();
		if (dbName[0] == '/') dbName = dbName.substr(1);
		enableCompression_ = connectData_.opts.enableCompression;
		Args args = {Arg{p_string(&userName)},
					 Arg{p_string(&password)},
					 Arg{p_string(&dbName)},
					 Arg{connectData_.opts.createDB},
					 Arg{connectData_.opts.hasExpectedClusterID},
					 Arg{connectData_.opts.expectedClusterID},
					 Arg{p_string(REINDEX_VERSION)},
					 Arg{p_string(&connectData_.opts.appName)}};
		constexpr uint32_t seq = 0;	 // login's seq num is always 0
		assert(buf.size() == 0);
		appendChunck(buf, packRPC(kCmdLogin, seq, args, Args{Arg{int64_t(0)}, Arg{int64_t(lsn_t())}, Arg{int64_t(-1)}}, std::nullopt).data);
		int err = 0;
		auto written = conn_.async_write(buf, err);
		auto toWrite = buf.size();
		buf.clear();
		if (err) {
			// TODO: handle reconnects
			return err > 0 ? Error(errNetwork, "Connection error: %s", strerror(err))
						   : Error(errNetwork, "Unable to write login cmd: connection closed");
		}
		assert(written == toWrite);
		(void)written;
		(void)toWrite;

		loop_->spawn(readWg_, [this] { readerRoutine(); });
	}
	return errOK;
}

void CoroClientConnection::handleFatalErrorFromReader(const Error &err) noexcept {
	if (errSyncCh_.opened() || terminate_) {
		return;
	} else {
		errSyncCh_.reopen();
	}
	conn_.close_conn(k_sock_closed_err);
	handleFatalErrorImpl(err);
}

void CoroClientConnection::handleFatalErrorImpl(const Error &err) noexcept {
	setLoggedIn(false);
	for (auto &c : rpcCalls_) {
		if (c.used && c.rspCh.opened() && !c.rspCh.full()) {
			c.rspCh.push(err);
		}
	}
	if (connectionStateHandler_) {
		connectionStateHandler_(err);
	}
	errSyncCh_.close();
}

void CoroClientConnection::handleFatalErrorFromWriter(const Error &err) noexcept {
	if (!terminate_) {
		if (errSyncCh_.opened()) {
			errSyncCh_.pop();
			return;
		} else {
			errSyncCh_.reopen();
		}
		conn_.close_conn(k_sock_closed_err);
		readWg_.wait();
		handleFatalErrorImpl(err);
	}
}

chunk CoroClientConnection::getChunk() noexcept {
	chunk ch;
	if (recycledChuncks_.size()) {
		ch = std::move(recycledChuncks_.back());
		ch.len_ = 0;
		ch.offset_ = 0;
		recycledChuncks_.pop_back();
	}
	return ch;
}

void CoroClientConnection::recycleChunk(chunk &&ch) noexcept {
	if (ch.cap_ <= kMaxChunckSizeToRecycle && recycledChuncks_.size() < kMaxRecycledChuncks) {
		recycledChuncks_.emplace_back(std::move(ch));
	}
}

void CoroClientConnection::writerRoutine() {
	std::vector<char> buf;
	buf.reserve(0x800);

	while (!terminate_) {
		size_t cnt = 0;
		do {
			auto mch = wrCh_.pop();
			if (!mch.second) {
				// channels is closed
				return;
			}
			if (mch.first.requiredLoginTs.has_value() && mch.first.requiredLoginTs != LoginTs()) {
				recycleChunk(std::move(mch.first.data));
				auto &c = rpcCalls_[mch.first.seq % rpcCalls_.size()];
				if (c.used && c.rspCh.opened() && !c.rspCh.full()) {
					c.rspCh.push(
						Error(errNetwork,
							  "Connection was broken and all corresponding snapshots, queryresults and transaction were invalidated"));
				}
				continue;
			}
			auto status = login(buf);
			if (!status.ok()) {
				recycleChunk(std::move(mch.first.data));
				handleFatalErrorFromWriter(status);
				continue;
			}
			appendChunck(buf, std::move(mch.first.data));
			++cnt;
		} while (cnt < kCntToSendNow && wrCh_.size());
		int err = 0;
		const bool sendNow = cnt == kCntToSendNow || buf.size() >= kDataToSendNow;
		auto written = conn_.async_write(buf, err, sendNow);
		if (err) {
			// disconnected
			buf.clear();
			handleFatalErrorFromWriter(Error(errNetwork, "Write error: %s", err > 0 ? strerror(err) : "Connection closed"));
			continue;
		}
		assert(written == buf.size());
		(void)written;
		buf.clear();
	}
}

void CoroClientConnection::readerRoutine() {
	CProtoHeader hdr;
	std::vector<char> buf;
	buf.reserve(kReadBufReserveSize);
	std::string uncompressed;
	do {
		buf.resize(sizeof(CProtoHeader));
		int err = 0;
		auto read = conn_.async_read(buf, sizeof(CProtoHeader), err);
		if (err) {
			// disconnected
			handleFatalErrorFromReader(err > 0 ? Error(errNetwork, "Read error: %s", strerror(err))
											   : Error(errNetwork, "Connection closed"));
			break;
		}
		assert(read == sizeof(hdr));
		(void)read;
		memcpy(&hdr, buf.data(), sizeof(hdr));

		if (hdr.magic != kCprotoMagic) {
			// disconnect
			handleFatalErrorFromReader(Error(errNetwork, "Invalid cproto magic=%08x", hdr.magic));
			break;
		}

		if (hdr.version < kCprotoMinCompatVersion) {
			// disconnect
			handleFatalErrorFromReader(
				Error(errParams, "Unsupported cproto version %04x. This client expects reindexer server v1.9.8+", int(hdr.version)));
			break;
		}

		buf.resize(hdr.len);
		read = conn_.async_read(buf, size_t(hdr.len), err);
		if (err) {
			// disconnected
			handleFatalErrorFromReader(err > 0 ? Error(errNetwork, "Read error: %s", strerror(err))
											   : Error(errNetwork, "Connection closed"));
			break;
		}
		assert(read == hdr.len);
		(void)read;

		CoroRPCAnswer ans;
		int errCode = 0;
		try {
			Serializer ser(buf.data(), hdr.len);
			if (hdr.compressed) {
				uncompressed.reserve(kReadBufReserveSize);
				if (!snappy::Uncompress(buf.data(), hdr.len, &uncompressed)) {
					throw Error(errParseBin, "Can't decompress data from peer");
				}
				ser = Serializer(uncompressed);
			}

			errCode = ser.GetVarUint();
			std::string_view errMsg = ser.GetVString();
			if (errCode != errOK) {
				ans.status_ = Error(errCode, errMsg);
			}
			ans.data_ = {ser.Buf() + ser.Pos(), ser.Len() - ser.Pos()};
		} catch (const Error &err) {
			// disconnect
			handleFatalErrorFromReader(err);
			break;
		}

		if (hdr.cmd == kCmdLogin) {
			if (ans.Status().ok()) {
				setLoggedIn(true);
				if (connectionStateHandler_) {
					connectionStateHandler_(Error());
				}
			} else {
				// disconnect
				handleFatalErrorFromReader(ans.Status());
			}
		} else if (hdr.cmd != kCmdUpdates) {
			auto &rpcData = rpcCalls_[hdr.seq % rpcCalls_.size()];
			if (!rpcData.used || rpcData.seq != hdr.seq) {
				auto cmdSv = CmdName(hdr.cmd);
				fprintf(stderr, "Unexpected RPC answer seq=%d cmd=%d(%.*s)\n", int(hdr.seq), hdr.cmd, int(cmdSv.size()), cmdSv.data());
				continue;
			}
			assert(rpcData.rspCh.opened());
			if (!rpcData.rspCh.readers()) {
				// In this case read buffer will be invalidated, before coroutine switch
				ans.EnsureHold(getChunk());
			}
			rpcData.rspCh.push(std::move(ans));
		} else {
			fprintf(stderr, "Unexpected updates response");
		}
	} while (loggedIn_ && !terminate_);
}

void CoroClientConnection::deadlineRoutine() {
	while (!terminate_) {
		loop_->granular_sleep(kDeadlineCheckInterval, kCoroSleepGranularity, terminate_);
		now_ += kDeadlineCheckInterval;

		for (auto &c : rpcCalls_) {
			if (!c.used) continue;
			bool expired = (c.deadline.time_since_epoch().count() && c.deadline <= now_);
			bool canceled = (c.cancelCtx && c.cancelCtx->IsCancelable() && (c.cancelCtx->GetCancelType() == CancelType::Explicit));
			if (expired || canceled) {
				if (c.rspCh.opened() && !c.rspCh.full()) {
					c.rspCh.push(Error(expired ? errTimeout : errCanceled, expired ? "Request deadline exceeded" : "Canceled"));
				}
			}
		}
	}
}

void CoroClientConnection::pingerRoutine() {
	const std::chrono::milliseconds timeout =
		connectData_.opts.keepAliveTimeout.count() > 0 ? connectData_.opts.keepAliveTimeout : kKeepAliveInterval;
	while (!terminate_) {
		loop_->granular_sleep(kKeepAliveInterval, kCoroSleepGranularity, terminate_);
		if (loggedIn_) {
			call({kCmdPing, timeout, milliseconds(0), lsn_t(), -1, ShardingKeyType::NotSetShard, nullptr, false}, {});
		}
	}
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
