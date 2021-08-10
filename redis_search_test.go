package beeorm

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisSearchIndexer(t *testing.T) {
	registry := &Registry{}
	testIndex := NewRedisSearchIndex("test", "search", []string{"doc:"})
	testIndex.AddTextField("title", 1, true, false, false)
	iteration := 0
	testIndex.Indexer = func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool) {
		iteration++
		if iteration == 1 {
			assert.Equal(t, uint64(0), lastID)
			return 7, true
		}
		assert.Equal(t, uint64(7), lastID)
		return 10, false
	}
	registry.RegisterRedisSearchIndex(testIndex)
	engine, def := prepareTables(t, registry, 5)
	defer def()

	indexer := NewBackgroundConsumer(engine)
	indexer.DisableLoop()
	indexer.blockTime = time.Millisecond
	indexer.Digest()
	assert.Equal(t, 2, iteration)

	iteration = 0
	testIndex.Indexer = func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool) {
		iteration++
		if iteration == 1 {
			assert.Equal(t, uint64(0), lastID)
			return 7, true
		}
		panic(fmt.Errorf("stop"))
	}
	engine.GetRedisSearch("search").ForceReindex("test")
	assert.PanicsWithError(t, "stop", func() {
		indexer.Digest()
	})
	assert.Equal(t, 2, iteration)
	iteration = 0
	testIndex.Indexer = func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool) {
		iteration++
		assert.Equal(t, uint64(7), lastID)
		return 10, false
	}
	indexer.Digest()
	assert.Equal(t, 1, iteration)
	iteration = 0
	testIndex.Indexer = func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool) {
		iteration++
		assert.Equal(t, uint64(0), lastID)
		return 0, false
	}
	engine.GetRedisSearch("search").ForceReindex("test")
	indexer.Digest()
	assert.Equal(t, 1, iteration)
	testIndex.Indexer = func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool) {
		return 10, true
	}
	engine.GetRedisSearch("search").ForceReindex("test")
	assert.PanicsWithError(t, "loop detected in indxer for index test in pool search", func() {
		indexer.Digest()
	})
}

func TestRedisSearch(t *testing.T) {
	registry := &Registry{}
	testIndex := NewRedisSearchIndex("test", "search", []string{"doc1:", "doc2:"})
	testIndex.PayloadField = "_my_payload"
	testIndex.ScoreField = "_my_score"
	testIndex.LanguageField = "_my_language"
	testIndex.DefaultScore = 0.8
	testIndex.StopWords = []string{"and", "in"}

	testIndex.MaxTextFields = true
	testIndex.NoOffsets = true
	testIndex.NoNHL = true // TODO why not visible in info
	testIndex.NoFields = true
	testIndex.NoFreqs = true
	testIndex.AddTextField("title", 0.4, true, false, false)
	testIndex.AddTextField("test", 1, false, true, true)
	testIndex.AddNumericField("age", true, false)
	testIndex.AddGeoField("location", false, false)
	testIndex.AddTagField("tags", true, false, ".")
	testIndex.Indexer = func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool) {
		return 0, false
	}
	registry.RegisterRedisSearchIndex(testIndex)
	testIndex2 := &RedisSearchIndex{Name: "test2", RedisPool: "search", Prefixes: []string{"test2:"}}
	testIndex2.AddTextField("title", 1, true, false, false)
	registry.RegisterRedisSearchIndex(testIndex2)
	defaultIndex := &RedisSearchIndex{Name: "default", RedisPool: "search"}
	defaultIndex.AddTextField("text_field", 0.12, true, false, false)
	defaultIndex.AddTagField("tag_field", true, false, ",")
	registry.RegisterRedisSearchIndex(defaultIndex)
	engine, def := prepareTables(t, registry, 5)
	defer def()

	testLog := &testLogHandler{}
	engine.RegisterQueryLogger(testLog, false, true, false)

	search := engine.GetRedisSearch("search")
	assert.NotNil(t, search)
	alters := engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 0)

	testLog.clear()
	search.createIndex(&RedisSearchIndex{Name: "to_delete", RedisPool: "search"})

	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	assert.Equal(t, "search", alters[0].Pool)
	assert.Equal(t, "to_delete", alters[0].Name)
	assert.Equal(t, "FT.DROPINDEX to_delete", alters[0].Query)
	alters[0].Execute()
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 0)

	indexer := NewBackgroundConsumer(engine)
	indexer.DisableLoop()
	indexer.blockTime = time.Millisecond
	indexer.Digest()

	info := search.Info("test")
	assert.Equal(t, "test", info.Name)
	assert.Equal(t, "_my_payload", info.Definition.PayloadField)
	assert.Equal(t, "_my_score", info.Definition.ScoreField)
	assert.Equal(t, "_my_language", info.Definition.LanguageField)
	assert.Equal(t, 0.8, info.Definition.DefaultScore)
	assert.Len(t, info.Definition.Prefixes, 2)
	assert.Equal(t, "doc1:", info.Definition.Prefixes[0])
	assert.Equal(t, "doc2:", info.Definition.Prefixes[1])
	assert.Equal(t, []string{"and", "in"}, info.StopWords)
	assert.True(t, info.Options.MaxTextFields)
	assert.True(t, info.Options.NoOffsets)
	assert.True(t, info.Options.NoFields)
	assert.True(t, info.Options.NoFreqs)
	assert.Len(t, info.Fields, 5)
	assert.Equal(t, "title", info.Fields[0].Name)
	assert.Equal(t, "TEXT", info.Fields[0].Type)
	assert.Equal(t, 0.4, info.Fields[0].Weight)
	assert.True(t, info.Fields[0].Sortable)
	assert.False(t, info.Fields[0].NoIndex)
	assert.False(t, info.Fields[0].NoStem)
	assert.Equal(t, "test", info.Fields[1].Name)
	assert.Equal(t, "TEXT", info.Fields[1].Type)
	assert.Equal(t, 1.0, info.Fields[1].Weight)
	assert.False(t, info.Fields[1].Sortable)
	assert.True(t, info.Fields[1].NoIndex)
	assert.True(t, info.Fields[1].NoStem)
	assert.Equal(t, "age", info.Fields[2].Name)
	assert.Equal(t, "NUMERIC", info.Fields[2].Type)
	assert.True(t, info.Fields[2].Sortable)
	assert.Equal(t, "location", info.Fields[3].Name)
	assert.Equal(t, "GEO", info.Fields[3].Type)
	assert.False(t, info.Fields[3].Sortable)
	assert.Equal(t, "tags", info.Fields[4].Name)
	assert.Equal(t, "TAG", info.Fields[4].Type)
	assert.True(t, info.Fields[4].Sortable)
	assert.Equal(t, ".", info.Fields[4].TagSeparator)

	testIndex2.Indexer = func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool) {
		for i := lastID + 1; i <= lastID+100; i++ {
			id := strconv.Itoa(int(i))
			pusher.NewDocument("test2:" + id)
			pusher.SetString("title", "hello "+id)
			pusher.SetInt("id", int64(i))
			pusher.PushDocument()
			newID = i
		}
		return newID, newID < 1000
	}
	search.ForceReindex("test2")
	indexer.Digest()

	testIndex2.AddTextField("title2", 1, false, false, false)
	testIndex2.AddNumericField("id", true, false)
	testIndex2.AddNumericField("number_signed", false, false)
	testIndex2.AddNumericField("number_float", true, false)
	testIndex2.AddNumericField("sort_test", true, false)
	testIndex2.AddGeoField("location", true, false)
	testIndex2.AddTagField("status", true, false, ",")
	testIndex2.LanguageField = "lang"
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	alters[0].Execute()
	time.Sleep(time.Millisecond * 100)

	search.ForceReindex("test2")
	indexer.Digest()
	time.Sleep(time.Millisecond * 100)

	pusher := engine.NewRedisSearchIndexPusher("search")
	pusher.NewDocument("test2:33")
	pusher.SetInt("number_signed", -10)
	pusher.SetFloat("number_float", 2.5)
	pusher.SetGeo("location", 52.2982648, 17.0103596)
	pusher.SetInt("sort_test", 30)
	pusher.SetString("title2", "hello 33 friend tom")
	pusher.SetTag("status", "active", "temporary")
	pusher.PushDocument()
	pusher.NewDocument("test2:34")
	pusher.SetInt("number_signed", 10)
	pusher.SetFloat("number_float", 7.34)
	pusher.SetGeo("location", 52.5248822, 17.5681129)
	pusher.SetInt("sort_test", 30)
	pusher.SetTag("status", "inactive", "temporary")
	pusher.PushDocument()
	pusher.NewDocument("test2:35")
	pusher.SetInt("number_signed", 5)
	pusher.SetFloat("number_float", 8.12)
	pusher.SetGeo("location", 52.2328546, 20.9207698)
	pusher.SetInt("sort_test", 20)
	pusher.SetTag("status", "")
	pusher.PushDocument()
	pusher.Flush()

	query := &RedisSearchQuery{}
	query.Query("hello").Verbatim().NoStopWords().Sort("id", false)

	total, rowsRaw := search.SearchRaw("test2", query, NewPager(1, 2))
	assert.Len(t, rowsRaw, 4)
	assert.Equal(t, uint64(1000), total)

	total, keys := search.SearchKeys("test2", query, NewPager(1, 2))
	assert.Len(t, keys, 2)
	assert.Equal(t, uint64(1000), total)
	assert.Equal(t, "test2:1", keys[0])
	assert.Equal(t, "test2:2", keys[1])

	query.FilterIntMinMax("id", 34, 35)
	_, rows := search.Search("test2", query, NewPager(1, 2))
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:34", rows[0].Key)
	assert.Equal(t, "hello 34", rows[0].Value("title"))
	assert.Equal(t, "34", rows[0].Value("id"))
	assert.Equal(t, "10", rows[0].Value("number_signed"))
	assert.Equal(t, "7.34", rows[0].Value("number_float"))
	assert.Equal(t, "52.524882,17.568113", rows[0].Value("location"))
	assert.Equal(t, "test2:35", rows[1].Key)
	assert.Equal(t, "35", rows[1].Value("id"))
	assert.Equal(t, "5", rows[1].Value("number_signed"))
	assert.Equal(t, "8.12", rows[1].Value("number_float"))
	assert.Equal(t, "52.232855,20.920770", rows[1].Value("location"))

	query.WithScores().WithPayLoads()
	_, rows = search.Search("test2", query, NewPager(1, 2))
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:34", rows[0].Key)
	assert.Equal(t, "test2:35", rows[1].Key)
	assert.Equal(t, "hello 34", rows[0].Value("title"))
	assert.Equal(t, "hello 35", rows[1].Value("title"))

	//engine.EnableQueryDebug()
	query = &RedisSearchQuery{}
	query.FilterInt("id", 34, 37)
	query.Sort("id", false)
	total, rows = search.Search("test2", query, NewPager(1, 2))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:34", rows[0].Key)
	assert.Equal(t, "test2:37", rows[1].Key)

	query = &RedisSearchQuery{}
	query.FilterIntMinMax("id", 33, 35).FilterIntMinMax("number_signed", -10, 5)
	total, rows = search.Search("test2", query, NewPager(1, 2))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:35", rows[0].Key)
	assert.Equal(t, "test2:33", rows[1].Key)

	query = &RedisSearchQuery{}
	query.FilterFloatMinMax("number_float", 7.33, 7.35)
	total, rows = search.Search("test2", query, NewPager(1, 2))
	assert.Equal(t, uint64(1), total)
	assert.Len(t, rows, 1)
	assert.Equal(t, "test2:34", rows[0].Key)

	query = &RedisSearchQuery{}
	query.FilterGeo("location", 52.2982648, 17.0103596, 75, "km")
	total, rows = search.Search("test2", query, NewPager(1, 2))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:34", rows[0].Key)
	assert.Equal(t, "test2:33", rows[1].Key)

	query = &RedisSearchQuery{}
	query.FilterIntMinMax("id", 1, 100).Sort("id", false)
	total, rows = search.Search("test2", query, NewPager(1, 3))
	assert.Equal(t, uint64(100), total)
	assert.Len(t, rows, 3)
	assert.Equal(t, "test2:1", rows[0].Key)
	assert.Equal(t, "test2:2", rows[1].Key)
	assert.Equal(t, "test2:3", rows[2].Key)

	query = &RedisSearchQuery{}
	query.FilterIntMinMax("id", 1, 100).Sort("id", true)
	total, rows = search.Search("test2", query, NewPager(1, 3))
	assert.Equal(t, uint64(100), total)
	assert.Len(t, rows, 3)
	assert.Equal(t, "test2:100", rows[0].Key)
	assert.Equal(t, "test2:99", rows[1].Key)
	assert.Equal(t, "test2:98", rows[2].Key)

	query.InKeys("test2:100", "test2:98")
	query.FilterIntMinMax("id", 1, 100).Sort("id", true)
	total, rows = search.Search("test2", query, NewPager(1, 3))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:100", rows[0].Key)
	assert.Equal(t, "test2:98", rows[1].Key)

	query = &RedisSearchQuery{}
	query.Query("hello").InFields("title2").Return("id", "title2")
	total, rows = search.Search("test2", query, NewPager(1, 3))
	assert.Equal(t, uint64(1), total)
	assert.Len(t, rows, 1)
	assert.Equal(t, "test2:33", rows[0].Key)
	assert.Len(t, rows[0].Fields, 4)
	assert.Equal(t, "id", rows[0].Fields[0])
	assert.Equal(t, "33", rows[0].Fields[1])
	assert.Equal(t, "title2", rows[0].Fields[2])
	assert.Equal(t, "hello 33 friend tom", rows[0].Fields[3])

	query = &RedisSearchQuery{}
	query.Query("hello tom").WithScores().ExplainScore()
	total, rows = search.Search("test2", query, NewPager(1, 3))
	assert.Equal(t, uint64(1), total)
	assert.GreaterOrEqual(t, rows[0].Score, 1.33)
	assert.NotNil(t, rows[0].ExplainScore)
	assert.Equal(t, "test2:33", rows[0].Key)
	query.Slop(0)
	total, _ = search.Search("test2", query, NewPager(1, 3))
	assert.Equal(t, uint64(0), total)

	query = &RedisSearchQuery{}
	query.Query("tom hello")
	total, _ = search.Search("test2", query, NewPager(1, 3))
	assert.Equal(t, uint64(1), total)
	query.InOrder()
	total, _ = search.Search("test2", query, NewPager(1, 3))
	assert.Equal(t, uint64(0), total)

	query = &RedisSearchQuery{}
	query.Query("hello").Lang("german")
	total, _ = search.Search("test2", query, NewPager(1, 3))
	assert.Equal(t, uint64(1000), total)

	query = &RedisSearchQuery{}
	query.Query("hello").Highlight("title").HighlightTags("<strong>", "</strong>").
		FilterInt("id", 33)
	total, rows = search.Search("test2", query, NewPager(1, 1))
	assert.Equal(t, uint64(1), total)
	assert.Equal(t, "<strong>hello</strong> 33", rows[0].Value("title"))
	assert.Equal(t, "hello 33 friend tom", rows[0].Value("title2"))

	query = &RedisSearchQuery{}
	query.Query("hello tom").Highlight().FilterInt("id", 33)
	total, rows = search.Search("test2", query, NewPager(1, 1))
	assert.Equal(t, uint64(1), total)
	assert.Equal(t, "<b>hello</b> 33", rows[0].Value("title"))
	assert.Equal(t, "<b>hello</b> 33 friend <b>tom</b>", rows[0].Value("title2"))

	query = &RedisSearchQuery{}
	query.Query("hello tom").Summarize("title2").FilterInt("id", 33)
	total, rows = search.Search("test2", query, NewPager(1, 1))
	assert.Equal(t, uint64(1), total)
	assert.Equal(t, "hello 33", rows[0].Value("title"))
	assert.Equal(t, "hello 33 friend tom... ", rows[0].Value("title2"))

	query.Query("hello tom").SummarizeOptions("...", 1, 2)
	total, rows = search.Search("test2", query, NewPager(1, 1))
	assert.Equal(t, uint64(1), total)
	assert.Equal(t, "hello 33", rows[0].Value("title"))
	assert.Equal(t, "hello 33 friend tom...", rows[0].Value("title2"))

	query = &RedisSearchQuery{}
	query.QueryRaw("@status: {temporary}").Sort("id", false)
	total, rows = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(2), total)
	assert.Equal(t, "33", rows[0].Value("id"))
	assert.Equal(t, "34", rows[1].Value("id"))

	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.StopWords = []string{"bb"}
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different stop words", alters[0].Changes[0])
	defaultIndex.StopWords = nil
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.StopWords = []string{}
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.LanguageField = "_my_lang"
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different language field", alters[0].Changes[0])
	defaultIndex.LanguageField = ""
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.LanguageField = "__language"
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.ScoreField = "score"
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different score field", alters[0].Changes[0])
	defaultIndex.ScoreField = "__score"
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.ScoreField = ""
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.PayloadField = "payload"
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different payload field", alters[0].Changes[0])
	defaultIndex.PayloadField = "__payload"
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.PayloadField = ""
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.Prefixes = []string{"test1:", "test2:"}
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different prefixes", alters[0].Changes[0])
	defaultIndex.Prefixes = []string{}
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.Prefixes = []string{""}
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.Prefixes = nil
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.Filter = "@indexName==\"myindex\""
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different filter", alters[0].Changes[0])
	defaultIndex.Filter = ""
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.NoFreqs = true
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different option NOFREQS", alters[0].Changes[0])
	defaultIndex.NoFreqs = false
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.NoFields = true
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different option NOFIELDS", alters[0].Changes[0])
	defaultIndex.NoFields = false
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.NoOffsets = true
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different option NOOFFSETS", alters[0].Changes[0])
	defaultIndex.NoOffsets = false
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.MaxTextFields = true
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different option MAXTEXTFIELDS", alters[0].Changes[0])
	defaultIndex.MaxTextFields = false
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.DefaultScore = 0.6
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different default score", alters[0].Changes[0])
	defaultIndex.DefaultScore = 1
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)
	defaultIndex.DefaultScore = 0
	assert.Len(t, engine.GetRedisSearchIndexAlters(), 0)

	defaultIndex.AddTextField("new_field", 0.2, false, false, false)
	defaultIndex.AddTagField("new_tag", false, false, ",")
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 2)
	assert.Equal(t, "new field new_field", alters[0].Changes[0])
	assert.Equal(t, "new field new_tag", alters[0].Changes[1])
	defaultIndex.Fields = defaultIndex.Fields[0:2]
	defaultIndex.Fields[0].Type = redisSearchIndexFieldTAG
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different field type text_field", alters[0].Changes[0])
	defaultIndex.Fields[0].Type = redisSearchIndexFieldText
	defaultIndex.Fields[0].Sortable = false
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different field sortable text_field", alters[0].Changes[0])
	defaultIndex.Fields[0].Sortable = true
	defaultIndex.Fields[0].NoIndex = true
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different field noindex text_field", alters[0].Changes[0])
	defaultIndex.Fields[0].NoIndex = false
	defaultIndex.Fields[0].NoStem = true
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different field nostem text_field", alters[0].Changes[0])
	defaultIndex.Fields[0].NoStem = false
	defaultIndex.Fields[0].Weight = 0.11
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different field weight text_field", alters[0].Changes[0])
	defaultIndex.Fields[0].Weight = 0.12
	defaultIndex.Fields[1].TagSeparator = "|"
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "different field separator tag_field", alters[0].Changes[0])
	defaultIndex.Fields[1].TagSeparator = ","
	defaultIndex.Fields = defaultIndex.Fields[1:2]
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters[0].Changes, 1)
	assert.Equal(t, "unneeded field text_field", alters[0].Changes[0])

	pusher.NewDocument("test2:100")
	pusher.SetString("title", "luna")
	pusher.SetUint("id", 100)
	pusher.PushDocument()
	pusher.Flush()
	query = &RedisSearchQuery{}
	query.FilterString("title", "luna")
	total, rows = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(1), total)
	assert.Equal(t, "luna", rows[0].Value("title"))

	pusher.NewDocument("test2:101")
	pusher.SetString("title", "adam@gmail.com")
	pusher.SetInt("id", 101)
	pusher.PushDocument()
	pusher.Flush()
	query = &RedisSearchQuery{}
	query.FilterString("title", "adam@gmail.com")
	total, rows = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(1), total)
	assert.Equal(t, "adam@gmail.com", rows[0].Value("title"))

	query = &RedisSearchQuery{}
	query.QueryRaw("@title: " + EscapeRedisSearchString("adam@gmail.com"))
	total, rows = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(1), total)
	assert.Equal(t, "adam@gmail.com", rows[0].Value("title"))

	pusher.NewDocument("test2:201")
	pusher.SetString("title", "tom has big house")
	pusher.SetInt("id", 201)
	pusher.PushDocument()
	pusher.NewDocument("test2:202")
	pusher.SetString("title", "tom has small house")
	pusher.SetInt("id", 202)
	pusher.PushDocument()
	pusher.Flush()

	query = &RedisSearchQuery{}
	query.FilterString("title", "has house")
	total, _ = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(0), total)

	query = &RedisSearchQuery{}
	query.FilterString("title", "big house")
	total, _ = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(1), total)

	query = &RedisSearchQuery{}
	query.QueryField("title", "has house")
	total, _ = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(2), total)

	pusher.DeleteDocuments("test2:201")
	pusher.Flush()

	query = &RedisSearchQuery{}
	query.QueryField("title", "has house")
	total, _ = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(1), total)

	assert.PanicsWithError(t, "unknown index invalid in pool search", func() {
		search.ForceReindex("invalid")
	})

	query = &RedisSearchQuery{}
	query.FilterString("title", "/`")
	total, _ = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(0), total)

	query = &RedisSearchQuery{}
	query.FilterString("title", "`")
	total, _ = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(0), total)

	query = &RedisSearchQuery{}
	query.FilterString("title", "_")
	total, _ = search.Search("test2", query, NewPager(1, 10))
	assert.Equal(t, uint64(0), total)

	engine.GetRedisSearch("search").ForceReindex("test2")
	delete(engine.registry.redisSearchIndexes["search"], "test2")
	indexer.Digest()
}
