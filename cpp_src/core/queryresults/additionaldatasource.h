#pragma once

#include "core/cjson/baseencoder.h"

namespace reindexer {

class AdditionalDatasource : public IAdditionalDatasource<JsonBuilder> {
public:
	AdditionalDatasource(double r, IEncoderDatasourceWithJoins *jds) : joinsDs_(jds), withRank_(true), rank_(r) {}
	AdditionalDatasource(IEncoderDatasourceWithJoins *jds) : joinsDs_(jds), withRank_(false), rank_(0.0) {}
	void PutAdditionalFields(JsonBuilder &builder) const final {
		if (withRank_) builder.Put("rank()", rank_);
	}
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return joinsDs_; }

private:
	IEncoderDatasourceWithJoins *joinsDs_;
	bool withRank_;
	double rank_;
};

}  // namespace reindexer
