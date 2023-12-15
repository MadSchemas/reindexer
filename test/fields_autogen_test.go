package reindexer

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItemAutogen struct {
	ID               int   `reindex:"id,,pk"`
	Genre            int64 `reindex:"genre,tree"`
	Age              int   `reindex:"age,hash"`
	UpdatedTime      int64 `reindex:"updated_time,-"`
	UpdatedTimeNano  int64 `reindex:"updated_time_nano,-"`
	UpdatedTimeMicro int64 `reindex:"updated_time_micro,-"`
	UpdatedTimeMilli int64 `reindex:"updated_time_milli,-"`
}

var ns = "test_items_autogen"

func init() {
	tnamespaces["test_items_autogen"] = TestItemAutogen{}
}

func TestAutogen(t *testing.T) {

	currentSerial := 0

	t.Run("field should be updated with current timestamp using NOW() function", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()"}
		item := TestItemAutogen{}
		err := DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
	})

	t.Run("field should contain different count of digits after 'nsec, usec, msec, sec' params usage", func(t *testing.T) {
		precepts := []string{"updated_time=NOW(sec)", "updated_time_milli=NOW(MSEC)", "updated_time_micro=now(usec)", "updated_time_nano=now(NSEC)"}
		item := TestItemAutogen{}
		err := DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)

		assert.Greater(t, item.UpdatedTimeMilli, time.Now().Unix()*1000)
		assert.Greater(t, item.UpdatedTimeMicro, time.Now().Unix()*1000000)
		assert.Greater(t, item.UpdatedTimeNano, time.Now().Unix()*1000000000)
	})

	t.Run("serial field shoud be increased by one", func(t *testing.T) {
		precepts := []string{"genre=SERIAL()", "age=serial()"}
		item := TestItemAutogen{}
		err := DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)
		currentSerial += 1

		assert.Equal(t, currentSerial, item.Age)
		assert.Equal(t, int64(currentSerial), item.Genre)

		t.Run("serial field should be increased by 5 after 5 iterations (must be equal 6 after previous test)", func(t *testing.T) {
			precepts := []string{"genre=SERIAL()", "age=serial()"}
			item := TestItemAutogen{}
			for i := 0; i < 5; i++ {
				err := DB.Upsert(ns, &item, precepts...)
				require.NoError(t, err)
				currentSerial += 1
			}

			assert.Equal(t, currentSerial, item.Age)
			assert.Equal(t, int64(currentSerial), item.Genre)
		})
	})

	t.Run("fill on insert, update, upsert", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()", "age=SERIAL()"}

		item := TestItemAutogen{ID: rand.Intn(100000000)}
		_, err := DB.Insert(ns, &item, precepts...)
		require.NoError(t, err)
		currentSerial += 1
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		assert.Equal(t, currentSerial, item.Age)

		item = TestItemAutogen{}
		err = DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)
		currentSerial += 1
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		assert.Equal(t, currentSerial, item.Age)

		item = TestItemAutogen{}
		_, err = DB.Update(ns, &item, precepts...)
		require.NoError(t, err)
		currentSerial += 1
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		assert.Equal(t, currentSerial, item.Age)
	})

	t.Run("fill on upsert nonexist item", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()", "age=SERIAL()"}
		item := TestItemAutogen{ID: rand.Intn(100000000)}

		err := DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)
		currentSerial += 1
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		assert.Equal(t, currentSerial, item.Age)
	})

	t.Run("doesn't fill on update nonexist item", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()", "age=SERIAL()"}
		item := TestItemAutogen{ID: rand.Intn(100000000)}

		count, err := DB.Update(ns, &item, precepts...)
		require.NoError(t, err)
		currentSerial += 1 // remove after 1602
		assert.Equal(t, 0, count)
		assert.Equal(t, int64(0), item.UpdatedTime)
		assert.Equal(t, 0, item.Age)
	})

	t.Run("doesn't fill on insert exist item", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()", "age=SERIAL()"}
		id := rand.Intn(100000000)
		item := TestItemAutogen{ID: id}

		count, err := DB.Insert(ns, &item, precepts...)
		require.NoError(t, err)
		currentSerial += 1
		assert.Equal(t, 1, count)
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		assert.Equal(t, currentSerial, item.Age)

		item = TestItemAutogen{ID: id}
		count, err = DB.Insert(ns, &item, precepts...)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, int64(0), item.UpdatedTime)
		assert.Equal(t, 0, item.Age)
	})
}
