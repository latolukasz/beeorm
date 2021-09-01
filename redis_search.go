package beeorm

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/go-redis/redis/v8"
)

const RedisSearchNullNumber = -math.MaxInt64

const redisSearchIndexFieldText = "TEXT"
const redisSearchIndexFieldNumeric = "NUMERIC"
const redisSearchIndexFieldGeo = "GEO"
const redisSearchIndexFieldTAG = "TAG"
const redisSearchForceIndexLastIDKeyPrefix = "_orm_force_index"

type RedisSearch struct {
	engine *Engine
	ctx    context.Context
	redis  *RedisCache
}

var redisSearchStringReplacer = strings.NewReplacer(",", "\\,", ".", "\\.", "<", "\\<", ">", "\\>", "{", "\\{",
	"}", "\\}", "[", "\\[", "]", "\\]", "\"", "\\\"", "'", "\\'", ":", "\\:", ";", "\\;", "!", "\\!", "@", "\\@",
	"#", "\\#", "$", "\\$", "%", "\\%", "^", "\\^", "&", "\\&", "*", "\\*", "(", "\\(", ")", "\\)", "-", "\\-",
	"+", "\\+", "=", "\\=", "~", "\\~", `/`, `\/`, "`", "\\`", `\`, `\\`, `|`, `\|`, `?`, `\?`)

var redisSearchStringReplacerBack = strings.NewReplacer("\\,", ",", "\\.", ".", "<", "<", "\\>", ">", "\\{", "{",
	"\\}", "}", "\\[", "[", "\\]", "]", "\\\"", "\"", "\\'", "'", "\\:", ":", "\\;", ";", "\\!", "!", "\\@", "@",
	"\\#", "#", "\\$", "$", "\\%", "%", "\\^", "^", "\\&", "&", "\\*", "*", "\\(", "(", "\\)", ")", "\\-", "-",
	"\\+", "+", "\\=", "=", "\\~", "~", `\/`, `/`, "\\`", "`", `\|`, `|`, `\?`, `?`)

var redisSearchStringReplacerOne = strings.NewReplacer(",", "\\,", ".", "\\.", "<", "\\<", ">", "\\>", "{", "\\{",
	"}", "\\}", "[", "\\[", "]", "\\]", "\"", "\\\"", "'", "\\'", ":", "\\:", ";", "\\;", "!", "\\!", "@", "\\@",
	"#", "\\#", "$", "\\$", "%", "\\%", "^", "\\^", "&", "\\&", "*", "\\*", "(", "\\(", ")", "\\)", "-", "\\-",
	"+", "\\+", "=", "\\=", "~", "\\~", `/`, `\/`, "`", "\\`", `_`, `\_`, `|`, `\|`, `\`, `\\`, `?`, `\?`)

var redisSearchStringReplacerBackOne = strings.NewReplacer("\\,", ",", "\\.", ".", "<", "<", "\\>", ">", "\\{", "{",
	"\\}", "}", "\\[", "[", "\\]", "]", "\\\"", "\"", "\\'", "'", "\\:", ":", "\\;", ";", "\\!", "!", "\\@", "@",
	"\\#", "#", "\\$", "$", "\\%", "%", "\\^", "^", "\\&", "&", "\\*", "*", "\\(", "(", "\\)", ")", "\\-", "-",
	"\\+", "+", "\\=", "=", "\\~", "~", `\/`, `/`, "\\`", "`", `\_`, `_`, `\|`, `|`, `\\`, `\`, `\?`, `?`)

type RedisSearchIndex struct {
	Name            string
	RedisPool       string
	Prefixes        []string
	DefaultLanguage string
	LanguageField   string
	DefaultScore    float64
	ScoreField      string
	MaxTextFields   bool
	NoOffsets       bool
	NoNHL           bool
	NoFields        bool
	NoFreqs         bool
	SkipInitialScan bool
	StopWords       []string
	Fields          []RedisSearchIndexField
	Indexer         RedisSearchIndexerFunc `json:"-"`
}

func NewRedisSearchIndex(name, pool string, prefixes []string) *RedisSearchIndex {
	return &RedisSearchIndex{
		Name:      name,
		RedisPool: pool,
		Prefixes:  prefixes,
	}
}

func (rs *RedisSearchIndex) AddTextField(name string, weight float64, sortable, noindex, nostem bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldText,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
		NoStem:   nostem,
		Weight:   weight,
	})
}

func (rs *RedisSearchIndex) AddNumericField(name string, sortable, noindex bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldNumeric,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
	})
}

func (rs *RedisSearchIndex) AddGeoField(name string, sortable, noindex bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldGeo,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
	})
}

func (rs *RedisSearchIndex) AddTagField(name string, sortable, noindex bool, separator string) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:         redisSearchIndexFieldTAG,
		Name:         name,
		Sortable:     sortable,
		NoIndex:      noindex,
		TagSeparator: separator,
	})
}

type RedisSearchIndexField struct {
	Type         string
	Name         string
	Sortable     bool
	NoIndex      bool
	NoStem       bool
	Weight       float64
	TagSeparator string
}

type RedisSearchIndexAlter struct {
	search    *RedisSearch
	Name      string
	Query     string
	Documents uint64
	Changes   []string
	Pool      string
	Execute   func()
}

type RedisSearchIndexInfoOptions struct {
	NoFreqs       bool
	NoOffsets     bool
	NoFields      bool
	MaxTextFields bool
}

type RedisSearchIndexInfo struct {
	Name                     string
	Options                  RedisSearchIndexInfoOptions
	Definition               RedisSearchIndexInfoDefinition
	Fields                   []RedisSearchIndexInfoField
	NumDocs                  uint64
	MaxDocID                 uint64
	NumTerms                 uint64
	NumRecords               uint64
	InvertedSzMB             float64
	TotalInvertedIndexBlocks float64
	OffsetVectorsSzMB        float64
	DocTableSizeMB           float64
	SortableValuesSizeMB     float64
	KeyTableSizeMB           float64
	RecordsPerDocAvg         int
	BytesPerRecordAvg        int
	OffsetsPerTermAvg        float64
	OffsetBitsPerRecordAvg   float64
	HashIndexingFailures     uint64
	Indexing                 bool
	PercentIndexed           float64
	StopWords                []string
}

type RedisSearchIndexInfoDefinition struct {
	KeyType       string
	Prefixes      []string
	LanguageField string
	ScoreField    string
	DefaultScore  float64
}

type RedisSearchIndexInfoField struct {
	Name         string
	Type         string
	Weight       float64
	Sortable     bool
	NoStem       bool
	NoIndex      bool
	TagSeparator string
}

func NewRedisSearchQuery() *RedisSearchQuery {
	return &RedisSearchQuery{}
}

type RedisSearchQuery struct {
	query              string
	filtersNumeric     map[string][][]string
	filtersNotNumeric  map[string][]string
	filtersGeo         map[string][]interface{}
	filtersTags        map[string][][]string
	filtersNotTags     map[string][][]string
	filtersString      map[string][][]string
	filtersNotString   map[string][][]string
	inKeys             []interface{}
	inFields           []interface{}
	toReturn           []interface{}
	sortDesc           bool
	sortField          string
	verbatim           bool
	noStopWords        bool
	withScores         bool
	slop               int
	inOrder            bool
	lang               string
	explainScore       bool
	highlight          []interface{}
	highlightOpenTag   string
	highlightCloseTag  string
	summarize          []interface{}
	summarizeSeparator string
	summarizeFrags     int
	summarizeLen       int
	withFakeDelete     bool
	hasFakeDelete      bool
}

type AggregateReduce struct {
	function string
	args     []interface{}
	alias    string
}

func NewAggregateReduceCount(alias string) AggregateReduce {
	return AggregateReduce{function: "COUNT", alias: alias}
}

func NewAggregateReduceCountDistinct(property, alias string, distinctish bool) AggregateReduce {
	f := "COUNT_DISTINCT"
	if distinctish {
		f += "ISH"
	}
	return AggregateReduce{function: f, args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceSum(property, alias string) AggregateReduce {
	return AggregateReduce{function: "SUM", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceMin(property, alias string) AggregateReduce {
	return AggregateReduce{function: "MIN", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceMax(property, alias string) AggregateReduce {
	return AggregateReduce{function: "MAX", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceAvg(property, alias string) AggregateReduce {
	return AggregateReduce{function: "AVG", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceStdDev(property, alias string) AggregateReduce {
	return AggregateReduce{function: "STDDEV", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceQuantile(property, quantile, alias string) AggregateReduce {
	return AggregateReduce{function: "QUANTILE", args: []interface{}{property, quantile}, alias: alias}
}

func NewAggregateReduceToList(property, alias string) AggregateReduce {
	return AggregateReduce{function: "TOLIST", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceFirstValue(property, alias string) AggregateReduce {
	return AggregateReduce{function: "FIRST_VALUE", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceFirstValueBy(property, byProperty, alias string, desc bool) AggregateReduce {
	sort := "ASC"
	if desc {
		sort = "DESC"
	}
	return AggregateReduce{function: "FIRST_VALUE", args: []interface{}{property, "BY", byProperty, sort}, alias: alias}
}

func NewAggregateReduceRandomSample(property, alias string, size ...int) AggregateReduce {
	sample := "1"
	if len(size) > 0 {
		sample = strconv.Itoa(size[0])
	}
	return AggregateReduce{function: "RANDOM_SAMPLE", args: []interface{}{property, sample}, alias: alias}
}

type RedisSearchAggregate struct {
	query *RedisSearchQuery
	args  []interface{}
}

type RedisSearchAggregateSort struct {
	Field string
	Desc  bool
}

type RedisSearchResult struct {
	Key          string
	Fields       []interface{}
	Score        float64
	ExplainScore []interface{}
}

func (a *RedisSearchAggregate) GroupByField(field string, reduce ...AggregateReduce) *RedisSearchAggregate {
	return a.GroupByFields([]string{field}, reduce...)
}

func (a *RedisSearchAggregate) Sort(fields ...RedisSearchAggregateSort) *RedisSearchAggregate {
	a.args = append(a.args, "SORTBY", strconv.Itoa(len(fields)*2))
	for _, field := range fields {
		if field.Desc {
			a.args = append(a.args, field.Field, "DESC")
		} else {
			a.args = append(a.args, field.Field, "ASC")
		}
	}
	return a
}

func (a *RedisSearchAggregate) Apply(expression, alias string) *RedisSearchAggregate {
	a.args = append(a.args, "APPLY", expression, "AS", alias)
	return a
}

func (a *RedisSearchAggregate) Filter(expression string) *RedisSearchAggregate {
	a.args = append(a.args, "FILTER", expression)
	return a
}

func (a *RedisSearchAggregate) GroupByFields(fields []string, reduce ...AggregateReduce) *RedisSearchAggregate {
	a.args = append(a.args, "GROUPBY", len(fields))
	for _, f := range fields {
		a.args = append(a.args, f)
	}
	for _, r := range reduce {
		a.args = append(a.args, "REDUCE", r.function, len(r.args))
		a.args = append(a.args, r.args...)
		a.args = append(a.args, "AS", r.alias)
	}

	return a
}

func (r *RedisSearchResult) Value(field string) interface{} {
	for i := 0; i < len(r.Fields); i += 2 {
		if r.Fields[i] == field {
			val := r.Fields[i+1]
			asString := val.(string)
			if len(asString) == 1 {
				return redisSearchStringReplacerBackOne.Replace(asString)
			}
			return redisSearchStringReplacerBack.Replace(asString)
		}
	}
	return nil
}

func (q *RedisSearchQuery) Query(query string) *RedisSearchQuery {
	q.query = EscapeRedisSearchString(query)
	return q
}

func (q *RedisSearchQuery) WithFakeDeleteRows() *RedisSearchQuery {
	q.withFakeDelete = true
	return q
}

func (q *RedisSearchQuery) QueryRaw(query string) *RedisSearchQuery {
	q.query = query
	return q
}

func (q *RedisSearchQuery) AppendQueryRaw(query string) *RedisSearchQuery {
	q.query += query
	return q
}

func (q *RedisSearchQuery) filterNumericMinMax(field string, min, max string) *RedisSearchQuery {
	if q.filtersNumeric == nil {
		q.filtersNumeric = make(map[string][][]string)
	}
	q.filtersNumeric[field] = append(q.filtersNumeric[field], []string{min, max})
	return q
}

func (q *RedisSearchQuery) filterNotNumeric(field string, val string) *RedisSearchQuery {
	if q.filtersNotNumeric == nil {
		q.filtersNotNumeric = make(map[string][]string)
	}
	q.filtersNotNumeric[field] = append(q.filtersNotNumeric[field], val)
	return q
}

func (q *RedisSearchQuery) FilterIntMinMax(field string, min, max int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatInt(min, 10), strconv.FormatInt(max, 10))
}

func (q *RedisSearchQuery) FilterInt(field string, value ...int64) *RedisSearchQuery {
	for _, val := range value {
		q.FilterIntMinMax(field, val, val)
	}
	return q
}

func (q *RedisSearchQuery) FilterNotInt(field string, value ...int64) *RedisSearchQuery {
	for _, val := range value {
		q.filterNotNumeric(field, strconv.FormatInt(val, 10))
	}
	return q
}

func (q *RedisSearchQuery) FilterIntNull(field string) *RedisSearchQuery {
	return q.FilterInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterNotIntNull(field string) *RedisSearchQuery {
	return q.FilterNotInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterIntGreaterEqual(field string, value int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatInt(value, 10), "+inf")
}

func (q *RedisSearchQuery) FilterIntGreater(field string, value int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatInt(value, 10), "+inf")
}

func (q *RedisSearchQuery) FilterIntLessEqual(field string, value int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatInt(value, 10))
}

func (q *RedisSearchQuery) FilterIntLess(field string, value int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatInt(value, 10))
}

func (q *RedisSearchQuery) FilterUintMinMax(field string, min, max uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatUint(min, 10), strconv.FormatUint(max, 10))
}

func (q *RedisSearchQuery) FilterUint(field string, value ...uint64) *RedisSearchQuery {
	for _, val := range value {
		q.FilterUintMinMax(field, val, val)
	}
	return q
}

func (q *RedisSearchQuery) FilterUintNull(field string) *RedisSearchQuery {
	return q.FilterInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterUintGreaterEqual(field string, value uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatUint(value, 10), "+inf")
}

func (q *RedisSearchQuery) FilterUintGreater(field string, value uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatUint(value, 10), "+inf")
}

func (q *RedisSearchQuery) FilterUintLessEqual(field string, value uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatUint(value, 10))
}

func (q *RedisSearchQuery) FilterUintLess(field string, value uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatUint(value, 10))
}

func (q *RedisSearchQuery) FilterString(field string, value ...string) *RedisSearchQuery {
	return q.filterString(field, true, false, false, value...)
}

func (q *RedisSearchQuery) FilterNotString(field string, value ...string) *RedisSearchQuery {
	return q.filterString(field, true, true, false, value...)
}

func (q *RedisSearchQuery) FilterManyReferenceIn(field string, id ...uint64) *RedisSearchQuery {
	values := q.buildRefMAnyValues(id)
	return q.filterString(field, false, false, false, values...)
}

func (q *RedisSearchQuery) FilterManyReferenceNotIn(field string, id ...uint64) *RedisSearchQuery {
	values := make([]string, len(id))
	for i, k := range id {
		values[i] = "e" + strconv.FormatUint(k, 10)
	}
	return q.filterString(field, false, true, false, values...)
}

func (q *RedisSearchQuery) QueryField(field string, value ...string) *RedisSearchQuery {
	return q.filterString(field, false, false, false, value...)
}

func (q *RedisSearchQuery) QueryFieldPrefixMatch(field string, value ...string) *RedisSearchQuery {
	return q.filterString(field, true, false, true, value...)
}

func (q *RedisSearchQuery) buildRefMAnyValues(id []uint64) []string {
	values := make([]string, len(id))
	for i, k := range id {
		values[i] = "e" + strconv.FormatUint(k, 10)
	}
	return values
}

func (q *RedisSearchQuery) filterString(field string, exactPhrase, not, starts bool, value ...string) *RedisSearchQuery {
	if len(value) == 0 {
		return q
	}
	if not {
		if q.filtersNotString == nil {
			q.filtersNotString = make(map[string][][]string)
		}
	} else {
		if q.filtersString == nil {
			q.filtersString = make(map[string][][]string)
		}
	}

	valueEscaped := make([]string, len(value))
	for i, v := range value {
		if v == "" {
			valueEscaped[i] = "\"NULL\""
		} else {
			if starts {
				values := strings.Split(strings.Trim(v, " "), " ")
				escaped := ""
				k := 0
				for _, val := range values {
					if len(val) >= 2 {
						if k > 0 {
							escaped += " "
						}
						escaped += EscapeRedisSearchString(val) + "*"
						k++
					}
				}
				if k == 0 {
					panic(fmt.Errorf("search start with requires min one word with 2 characters"))
				}
				valueEscaped[i] = escaped
			} else if exactPhrase {
				valueEscaped[i] = "\"" + EscapeRedisSearchString(v) + "\""
			} else {
				valueEscaped[i] = EscapeRedisSearchString(v)
			}
		}
	}
	if not {
		q.filtersNotString[field] = append(q.filtersNotString[field], valueEscaped)
	} else {
		q.filtersString[field] = append(q.filtersString[field], valueEscaped)
	}
	return q
}

func (q *RedisSearchQuery) FilterFloatMinMax(field string, min, max float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatFloat(min-0.00001, 'f', -1, 64),
		strconv.FormatFloat(max+0.00001, 'f', -1, 64))
}

func (q *RedisSearchQuery) FilterFloat(field string, value ...float64) *RedisSearchQuery {
	for _, val := range value {
		q.FilterFloatMinMax(field, val, val)
	}
	return q
}

func (q *RedisSearchQuery) FilterFloatGreaterEqual(field string, value float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatFloat(value-0.00001, 'f', -1, 64), "+inf")
}

func (q *RedisSearchQuery) FilterFloatGreater(field string, value float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatFloat(value+0.00001, 'f', -1, 64), "+inf")
}

func (q *RedisSearchQuery) FilterFloatLessEqual(field string, value float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatFloat(value+0.00001, 'f', -1, 64))
}

func (q *RedisSearchQuery) FilterFloatLess(field string, value float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatFloat(value-0.00001, 'f', -1, 64))
}

func (q *RedisSearchQuery) FilterFloatNull(field string) *RedisSearchQuery {
	return q.FilterFloat(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterDateMinMax(field string, min, max time.Time) *RedisSearchQuery {
	return q.FilterIntMinMax(field, q.cutDate(min), q.cutDate(max))
}

func (q *RedisSearchQuery) FilterDate(field string, date time.Time) *RedisSearchQuery {
	unix := q.cutDate(date)
	return q.FilterIntMinMax(field, unix, unix)
}

func (q *RedisSearchQuery) FilterNotDate(field string, date time.Time) *RedisSearchQuery {
	return q.filterNotNumeric(field, strconv.FormatInt(q.cutDate(date), 10))
}

func (q *RedisSearchQuery) FilterDateNull(field string) *RedisSearchQuery {
	return q.FilterInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterNotDateNull(field string) *RedisSearchQuery {
	return q.FilterNotInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterDateGreaterEqual(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatInt(q.cutDate(date), 10), "+inf")
}

func (q *RedisSearchQuery) FilterDateGreater(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatInt(q.cutDate(date), 10), "+inf")
}

func (q *RedisSearchQuery) FilterDateLessEqual(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatInt(q.cutDate(date), 10))
}

func (q *RedisSearchQuery) FilterDateLess(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatInt(q.cutDate(date), 10))
}

func (q *RedisSearchQuery) FilterDateTimeMinMax(field string, min, max time.Time) *RedisSearchQuery {
	return q.FilterIntMinMax(field, q.cutDateTime(min), q.cutDateTime(max))
}

func (q *RedisSearchQuery) FilterDateTime(field string, date time.Time) *RedisSearchQuery {
	unix := q.cutDateTime(date)
	return q.FilterIntMinMax(field, unix, unix)
}

func (q *RedisSearchQuery) FilterDateTimeNull(field string) *RedisSearchQuery {
	return q.FilterInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterDateTimeGreaterEqual(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatInt(q.cutDateTime(date), 10), "+inf")
}

func (q *RedisSearchQuery) FilterDateTimeGreater(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatInt(q.cutDateTime(date), 10), "+inf")
}

func (q *RedisSearchQuery) FilterDateTimeLessEqual(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatInt(q.cutDateTime(date), 10))
}

func (q *RedisSearchQuery) FilterDateTimeLess(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatInt(q.cutDateTime(date), 10))
}

func (q *RedisSearchQuery) FilterTag(field string, tag ...string) *RedisSearchQuery {
	if q.filtersTags == nil {
		q.filtersTags = make(map[string][][]string)
	}
	tagEscaped := make([]string, len(tag))
	for i, v := range tag {
		if v == "" {
			v = "NULL"
		} else {
			v = EscapeRedisSearchString(v)
		}
		tagEscaped[i] = v
	}
	q.filtersTags[field] = append(q.filtersTags[field], tagEscaped)
	return q
}

func (q *RedisSearchQuery) FilterNotTag(field string, tag ...string) *RedisSearchQuery {
	if q.filtersNotTags == nil {
		q.filtersNotTags = make(map[string][][]string)
	}
	tagEscaped := make([]string, len(tag))
	for i, v := range tag {
		if v == "" {
			v = "NULL"
		} else {
			v = EscapeRedisSearchString(v)
		}
		tagEscaped[i] = v
	}
	q.filtersNotTags[field] = append(q.filtersNotTags[field], tagEscaped)
	return q
}

func (q *RedisSearchQuery) FilterBool(field string, value bool) *RedisSearchQuery {
	if value {
		return q.FilterTag(field, "true")
	}
	return q.FilterTag(field, "false")
}

func (q *RedisSearchQuery) FilterGeo(field string, lon, lat, radius float64, unit string) *RedisSearchQuery {
	if q.filtersGeo == nil {
		q.filtersGeo = make(map[string][]interface{})
	}
	q.filtersGeo[field] = []interface{}{lon, lat, radius, unit}
	return q
}

func (q *RedisSearchQuery) Sort(field string, desc bool) *RedisSearchQuery {
	q.sortField = field
	q.sortDesc = desc
	return q
}

func (q *RedisSearchQuery) Aggregate() *RedisSearchAggregate {
	return &RedisSearchAggregate{query: q}
}

func (q *RedisSearchQuery) Verbatim() *RedisSearchQuery {
	q.verbatim = true
	return q
}

func (q *RedisSearchQuery) NoStopWords() *RedisSearchQuery {
	q.noStopWords = true
	return q
}

func (q *RedisSearchQuery) WithScores() *RedisSearchQuery {
	q.withScores = true
	return q
}

func (q *RedisSearchQuery) InKeys(key ...string) *RedisSearchQuery {
	for _, k := range key {
		q.inKeys = append(q.inKeys, k)
	}
	return q
}

func (q *RedisSearchQuery) InFields(field ...string) *RedisSearchQuery {
	for _, k := range field {
		q.inFields = append(q.inFields, k)
	}
	return q
}

func (q *RedisSearchQuery) Return(field ...string) *RedisSearchQuery {
	for _, k := range field {
		q.toReturn = append(q.toReturn, k)
	}
	return q
}

func (q *RedisSearchQuery) Slop(slop int) *RedisSearchQuery {
	q.slop = slop
	if q.slop == 0 {
		q.slop = -1
	}
	return q
}

func (q *RedisSearchQuery) InOrder() *RedisSearchQuery {
	q.inOrder = true
	return q
}

func (q *RedisSearchQuery) ExplainScore() *RedisSearchQuery {
	q.explainScore = true
	return q
}

func (q *RedisSearchQuery) Lang(lang string) *RedisSearchQuery {
	q.lang = lang
	return q
}

func (q *RedisSearchQuery) Highlight(field ...string) *RedisSearchQuery {
	if q.highlight == nil {
		q.highlight = make([]interface{}, 0)
	}
	for _, k := range field {
		q.highlight = append(q.highlight, k)
	}
	return q
}

func (q *RedisSearchQuery) HighlightTags(openTag, closeTag string) *RedisSearchQuery {
	q.highlightOpenTag = openTag
	q.highlightCloseTag = closeTag
	return q
}

func (q *RedisSearchQuery) Summarize(field ...string) *RedisSearchQuery {
	if q.summarize == nil {
		q.summarize = make([]interface{}, 0)
	}
	for _, k := range field {
		q.summarize = append(q.summarize, k)
	}
	return q
}

func (q *RedisSearchQuery) SummarizeOptions(separator string, frags, len int) *RedisSearchQuery {
	q.summarizeSeparator = separator
	q.summarizeFrags = frags
	q.summarizeLen = len
	return q
}

func (r *RedisSearch) ForceReindex(index string) {
	def, has := r.engine.registry.redisSearchIndexes[r.redis.config.GetCode()][index]
	if !has {
		panic(errors.Errorf("unknown index %s in pool %s", index, r.redis.config.GetCode()))
	}
	r.dropIndex(index, true)
	r.createIndex(def)
	event := redisIndexerEvent{Index: index}
	r.engine.GetEventBroker().Publish(redisSearchIndexerChannelName, event)
}

func (r *RedisSearch) SearchRaw(index string, query *RedisSearchQuery, pager *Pager) (total uint64, rows []interface{}) {
	return r.search(index, query, pager, false)
}

func (r *RedisSearch) Search(index string, query *RedisSearchQuery, pager *Pager) (total uint64, rows []*RedisSearchResult) {
	total, data := r.search(index, query, pager, false)
	rows = make([]*RedisSearchResult, 0)
	max := len(data) - 1
	i := 0
	for {
		if i > max {
			break
		}
		row := &RedisSearchResult{Key: data[i].(string)}
		if query.explainScore {
			i++
			row.ExplainScore = data[i].([]interface{})
			row.Score, _ = strconv.ParseFloat(row.ExplainScore[0].(string), 64)
			row.ExplainScore = row.ExplainScore[1].([]interface{})
		} else if query.withScores {
			i++
			row.Score, _ = strconv.ParseFloat(data[i].(string), 64)
		}
		i++
		row.Fields = data[i].([]interface{})
		rows = append(rows, row)
		i++
	}

	return total, rows
}

func (r *RedisSearch) SearchKeys(index string, query *RedisSearchQuery, pager *Pager) (total uint64, keys []string) {
	total, rows := r.search(index, query, pager, true)
	keys = make([]string, len(rows))
	for k, v := range rows {
		keys[k] = v.(string)
	}
	return total, keys
}

func (r *RedisSearch) Aggregate(index string, query *RedisSearchAggregate, pager *Pager) (result []map[string]string, totalRows uint64) {
	args := []interface{}{"FT.AGGREGATE", index}
	if query.query == nil {
		args = append(args, "*")
	} else {
		args = r.buildQueryArgs(query.query, args)
	}
	args = append(args, query.args...)
	args = r.applyPager(pager, args)
	cmd := redis.NewSliceCmd(r.ctx, args...)
	start := getNow(r.engine.hasRedisLogger)
	err := r.redis.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("FT.AGGREGATE", cmd.String(), start, err)
	}
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	totalRows = uint64(res[0].(int64))
	result = make([]map[string]string, totalRows)
	for i, row := range res[1:] {
		data := make(map[string]string)
		rowSlice := row.([]interface{})
		for k := 0; k < len(rowSlice); k = k + 2 {
			asSLice, ok := rowSlice[k+1].([]interface{})
			if ok {
				values := make([]string, len(asSLice))
				for k, v := range asSLice {
					values[k] = v.(string)
				}
				data[rowSlice[k].(string)] = strings.Join(values, ",")
			} else {
				data[rowSlice[k].(string)] = rowSlice[k+1].(string)
			}
		}
		result[i] = data
	}
	return result, totalRows
}

func (r *RedisSearch) applyPager(pager *Pager, args []interface{}) []interface{} {
	if pager != nil {
		if pager.PageSize > 10000 {
			panic(fmt.Errorf("pager size exceeded limit 10000"))
		}
		args = append(args, "LIMIT")
		args = append(args, (pager.CurrentPage-1)*pager.PageSize)
		args = append(args, pager.PageSize)
	} else {
		panic(fmt.Errorf("missing pager in redis search query"))
	}
	return args
}

func (r *RedisSearch) GetPoolConfig() RedisPoolConfig {
	return r.redis.config
}

func (r *RedisSearch) search(index string, query *RedisSearchQuery, pager *Pager, noContent bool) (total uint64, rows []interface{}) {
	args := []interface{}{"FT.SEARCH", index}
	args = r.buildQueryArgs(query, args)

	if noContent {
		args = append(args, "NOCONTENT")
	}
	if query.verbatim {
		args = append(args, "VERBATIM")
	}
	if query.noStopWords {
		args = append(args, "NOSTOPWORDS")
	}
	if query.withScores {
		args = append(args, "WITHSCORES")
	}
	if query.sortField != "" {
		args = append(args, "SORTBY", query.sortField)
		if query.sortDesc {
			args = append(args, "DESC")
		}
	}
	if len(query.inKeys) > 0 {
		args = append(args, "INKEYS", len(query.inKeys))
		args = append(args, query.inKeys...)
	}
	if len(query.inFields) > 0 {
		args = append(args, "INFIELDS", len(query.inFields))
		args = append(args, query.inFields...)
	}
	if len(query.toReturn) > 0 {
		args = append(args, "RETURN", len(query.toReturn))
		args = append(args, query.toReturn...)
	}
	if query.slop != 0 {
		slop := query.slop
		if slop == -1 {
			slop = 0
		}
		args = append(args, "SLOP", slop)
	}
	if query.inOrder {
		args = append(args, "INORDER")
	}
	if query.lang != "" {
		args = append(args, "LANGUAGE", query.lang)
	}
	if query.explainScore {
		args = append(args, "EXPLAINSCORE")
	}
	if query.highlight != nil {
		args = append(args, "HIGHLIGHT")
		if l := len(query.highlight); l > 0 {
			args = append(args, "FIELDS", l)
			args = append(args, query.highlight...)
		}
		if query.highlightOpenTag != "" && query.highlightCloseTag != "" {
			args = append(args, "TAGS", query.highlightOpenTag, query.highlightCloseTag)
		}
	}
	if query.summarize != nil {
		args = append(args, "SUMMARIZE")
		if l := len(query.summarize); l > 0 {
			args = append(args, "FIELDS", l)
			args = append(args, query.summarize...)
		}
		if query.summarizeFrags > 0 {
			args = append(args, "FRAGS", query.summarizeFrags)
		}
		if query.summarizeLen > 0 {
			args = append(args, "LEN", query.summarizeLen)
		}
		if query.summarizeSeparator != "" {
			args = append(args, "SEPARATOR", query.summarizeSeparator)
		}
	}
	args = r.applyPager(pager, args)
	cmd := redis.NewSliceCmd(r.ctx, args...)
	start := getNow(r.engine.hasRedisLogger)
	err := r.redis.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("FT.SEARCH", cmd.String(), start, err)
	}
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	total = uint64(res[0].(int64))
	return total, res[1:]
}

func (r *RedisSearch) buildQueryArgs(query *RedisSearchQuery, args []interface{}) []interface{} {
	q := query.query
	for field, in := range query.filtersNumeric {
		if len(in) == 1 {
			continue
		}
		if q != "" {
			q += " "
		}
		for i, v := range in {
			if i > 0 {
				q += "|"
			}
			q += "@" + field + ":"
			q += "[" + v[0] + " " + v[1] + "]"
		}
	}
	for field, in := range query.filtersTags {
		for _, v := range in {
			if q != "" {
				q += " "
			}
			q += "@" + field + ":{ " + strings.Join(v, " | ") + " }"
		}
	}
	for field, in := range query.filtersString {
		for _, v := range in {
			if q != "" {
				q += " "
			}
			q += "@" + field + ":( " + strings.Join(v, " | ") + " )"
		}
	}
	for field, in := range query.filtersNotNumeric {
		if q != "" {
			q += " "
		}
		for _, v := range in {
			q += "(@" + field + ":[-inf (" + v + "] | @" + field + ":[(" + v + " +inf])"
		}
	}
	for field, in := range query.filtersNotTags {
		for _, v := range in {
			if q != "" {
				q += " "
			}
			q += "-@" + field + ":{ " + strings.Join(v, " | ") + " }"
		}
	}
	for field, in := range query.filtersNotString {
		for _, v := range in {
			if q != "" {
				q += " "
			}
			q += "-@" + field + ":( " + strings.Join(v, " | ") + " )"
		}
	}
	if query.hasFakeDelete && !query.withFakeDelete {
		q += "-@FakeDelete:{true}"
	}
	if q == "" {
		q = "*"
	}
	args = append(args, q)

	for field, ranges := range query.filtersNumeric {
		if len(ranges) == 1 {
			args = append(args, "FILTER", field, ranges[0][0], ranges[0][1])
		}
	}
	for field, data := range query.filtersGeo {
		args = append(args, "GEOFILTER", field, data[0], data[1], data[2], data[3])
	}
	return args
}

func (r *RedisSearch) createIndexArgs(index *RedisSearchIndex, indexName string) []interface{} {
	args := []interface{}{"FT.CREATE", indexName, "ON", "HASH", "PREFIX", len(index.Prefixes)}
	for _, prefix := range index.Prefixes {
		args = append(args, prefix)
	}
	if index.DefaultLanguage != "" {
		args = append(args, "LANGUAGE", index.DefaultLanguage)
	}
	if index.LanguageField != "" {
		args = append(args, "LANGUAGE_FIELD", index.LanguageField)
	}
	if index.DefaultScore > 0 {
		args = append(args, "SCORE", index.DefaultScore)
	}
	if index.ScoreField != "" {
		args = append(args, "SCORE_FIELD", index.ScoreField)
	}
	if index.MaxTextFields {
		args = append(args, "MAXTEXTFIELDS")
	}
	if index.NoOffsets {
		args = append(args, "NOOFFSETS")
	}
	if index.NoNHL {
		args = append(args, "NOHL")
	}
	if index.NoFields {
		args = append(args, "NOFIELDS")
	}
	if index.NoFreqs {
		args = append(args, "NOFREQS")
	}
	if index.SkipInitialScan {
		args = append(args, "SKIPINITIALSCAN")
	}
	if len(index.StopWords) > 0 {
		args = append(args, "STOPWORDS", len(index.StopWords))
		for _, word := range index.StopWords {
			args = append(args, word)
		}
	}
	args = append(args, "SCHEMA")
	for _, field := range index.Fields {
		fieldArgs := []interface{}{field.Name, field.Type}
		if field.Type == redisSearchIndexFieldText {
			if field.NoStem {
				fieldArgs = append(fieldArgs, "NOSTEM")
			}
			if field.Weight != 1 {
				fieldArgs = append(fieldArgs, "WEIGHT", field.Weight)
			}
		} else if field.Type == redisSearchIndexFieldTAG {
			if field.TagSeparator != "" && field.TagSeparator != ", " {
				fieldArgs = append(fieldArgs, "SEPARATOR", field.TagSeparator)
			}
		}
		if field.Sortable {
			fieldArgs = append(fieldArgs, "SORTABLE")
		}
		if field.NoIndex {
			fieldArgs = append(fieldArgs, "NOINDEX")
		}
		args = append(args, fieldArgs...)
	}
	return args
}

func (r *RedisSearch) createIndex(index *RedisSearchIndex) {
	args := r.createIndexArgs(index, index.Name)
	cmd := redis.NewStringCmd(r.ctx, args...)

	start := getNow(r.engine.hasRedisLogger)
	err := r.redis.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("FT.CREATE", cmd.String(), start, err)
	}
	checkError(err)
}

func (r *RedisSearch) ListIndices() []string {
	cmd := redis.NewStringSliceCmd(r.ctx, "FT._LIST")
	start := getNow(r.engine.hasRedisLogger)
	err := r.redis.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("FT.LIST", "FT.LIST", start, err)
	}
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	return res
}

func (r *RedisSearch) dropIndex(indexName string, withHashes bool) bool {
	args := []interface{}{"FT.DROPINDEX", indexName}
	if withHashes {
		args = append(args, "DD")
	}
	cmd := redis.NewStringCmd(r.ctx, args...)
	start := getNow(r.engine.hasRedisLogger)
	err := r.redis.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("FT.DROPINDEX", cmd.String(), start, err)
	}
	if err != nil && strings.HasPrefix(err.Error(), "Unknown Index ") {
		return false
	}
	checkError(err)
	_, err = cmd.Result()
	checkError(err)
	return true
}

func (r *RedisSearch) Info(indexName string) *RedisSearchIndexInfo {
	cmd := redis.NewSliceCmd(r.ctx, "FT.INFO", indexName)
	start := getNow(r.engine.hasRedisLogger)
	err := r.redis.client.Process(r.ctx, cmd)
	has := true
	if err != nil && err.Error() == "Unknown Index name" {
		err = nil
		has = false
	}
	if r.engine.hasRedisLogger {
		r.fillLogFields("FT.INFO", "FT.INFO "+indexName, start, err)
	}
	if !has {
		return nil
	}
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	info := &RedisSearchIndexInfo{}
	for i, row := range res {
		switch row {
		case "index_name":
			info.Name = res[i+1].(string)
		case "index_options":
			infoOptions := res[i+1].([]interface{})
			options := RedisSearchIndexInfoOptions{}
			for _, opt := range infoOptions {
				switch opt {
				case "NOFREQS":
					options.NoFreqs = true
				case "NOFIELDS":
					options.NoFields = true
				case "NOOFFSETS":
					options.NoOffsets = true
				case "MAXTEXTFIELDS":
					options.MaxTextFields = true
				}
			}
			info.Options = options
		case "index_definition":
			def := res[i+1].([]interface{})
			definition := RedisSearchIndexInfoDefinition{}
			for subKey, subValue := range def {
				switch subValue {
				case "key_type":
					definition.KeyType = def[subKey+1].(string)
				case "prefixes":
					prefixesRaw := def[subKey+1].([]interface{})
					prefixes := make([]string, len(prefixesRaw))
					for k, v := range prefixesRaw {
						prefixes[k] = v.(string)
					}
					definition.Prefixes = prefixes
				case "language_field":
					definition.LanguageField = def[subKey+1].(string)
				case "default_score":
					score, _ := strconv.ParseFloat(def[subKey+1].(string), 64)
					definition.DefaultScore = score
				case "score_field":
					definition.ScoreField = def[subKey+1].(string)
				}
			}
			info.Definition = definition
		case "fields":
			fieldsRaw := res[i+1].([]interface{})
			fields := make([]RedisSearchIndexInfoField, len(fieldsRaw))
			for i, v := range fieldsRaw {
				def := v.([]interface{})
				field := RedisSearchIndexInfoField{Name: def[0].(string)}
				def = def[1:]
				for subKey, subValue := range def {
					switch subValue {
					case "type":
						field.Type = def[subKey+1].(string)
					case "WEIGHT":
						weight, _ := strconv.ParseFloat(def[subKey+1].(string), 64)
						field.Weight = weight
					case "SORTABLE":
						field.Sortable = true
					case "NOSTEM":
						field.NoStem = true
					case "NOINDEX":
						field.NoIndex = true
					case "SEPARATOR":
						field.TagSeparator = def[subKey+1].(string)
					}
				}
				fields[i] = field
			}
			info.Fields = fields
		case "num_docs":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.NumDocs = v
		case "max_doc_id":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.MaxDocID = v
		case "num_terms":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.NumTerms = v
		case "num_records":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.NumRecords = v
		case "inverted_sz_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.InvertedSzMB = v
		case "total_inverted_index_blocks":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.TotalInvertedIndexBlocks = v
		case "offset_vectors_sz_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.OffsetVectorsSzMB = v
		case "doc_table_size_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.DocTableSizeMB = v
		case "sortable_values_size_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.SortableValuesSizeMB = v
		case "key_table_size_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.KeyTableSizeMB = v
		case "records_per_doc_avg":
			if res[i+1] != "-nan" {
				info.RecordsPerDocAvg, _ = strconv.Atoi(res[i+1].(string))
			}
		case "bytes_per_record_avg":
			if res[i+1] != "-nan" {
				info.BytesPerRecordAvg, _ = strconv.Atoi(res[i+1].(string))
			}
		case "offsets_per_term_avg":
			if res[i+1] != "-nan" {
				v, _ := strconv.ParseFloat(res[i+1].(string), 64)
				info.OffsetsPerTermAvg = v
			}
		case "offset_bits_per_record_avg":
			if res[i+1] != "-nan" {
				v, _ := strconv.ParseFloat(res[i+1].(string), 64)
				info.OffsetBitsPerRecordAvg = v
			}
		case "hash_indexing_failures":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.HashIndexingFailures = v
		case "indexing":
			info.Indexing = res[i+1] == "1"
		case "percent_indexed":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.PercentIndexed = v
		case "stopwords_list":
			v := res[i+1].([]interface{})
			info.StopWords = make([]string, len(v))
			for i, v := range v {
				info.StopWords[i] = v.(string)
			}
		}
	}
	return info
}

func EscapeRedisSearchString(val string) string {
	if len(val) == 1 {
		return redisSearchStringReplacerOne.Replace(val)
	}
	return redisSearchStringReplacer.Replace(val)
}

func getRedisSearchAlters(engine *Engine) (alters []RedisSearchIndexAlter) {
	alters = make([]RedisSearchIndexAlter, 0)
	for _, pool := range engine.GetRegistry().GetRedisPools() {
		poolName := pool.GetCode()
		r := engine.GetRedis(poolName)
		if r.GetPoolConfig().GetDatabase() > 0 {
			continue
		}
		search := engine.GetRedisSearch(poolName)
		inRedis := make(map[string]bool)
		for _, name := range search.ListIndices() {
			def, has := engine.registry.redisSearchIndexes[poolName][name]
			if !has {
				alter := RedisSearchIndexAlter{Pool: poolName, Query: "FT.DROPINDEX " + name, Name: name, search: search}
				nameToRemove := name
				alter.Execute = func() {
					alter.search.dropIndex(nameToRemove, false)
				}
				alter.Documents = search.Info(name).NumDocs
				alters = append(alters, alter)
				continue
			}
			inRedis[name] = true
			info := search.Info(name)
			changes := make([]string, 0)
			stopWords := def.StopWords
			if len(stopWords) == 0 {
				stopWords = nil
			}
			if !reflect.DeepEqual(info.StopWords, stopWords) {
				changes = append(changes, "different stop words")
			}
			prefixes := def.Prefixes
			if len(prefixes) == 0 || (len(prefixes) == 1 && prefixes[0] == "") {
				prefixes = []string{""}
			}
			if !reflect.DeepEqual(info.Definition.Prefixes, prefixes) {
				changes = append(changes, "different prefixes")
			}
			languageField := def.LanguageField
			if languageField == "" {
				languageField = "__language"
			}
			if info.Definition.LanguageField != languageField {
				changes = append(changes, "different language field")
			}
			scoreField := def.ScoreField
			if scoreField == "" {
				scoreField = "__score"
			}
			if info.Definition.ScoreField != scoreField {
				changes = append(changes, "different score field")
			}
			if info.Options.NoFreqs != def.NoFreqs {
				changes = append(changes, "different option NOFREQS")
			}
			if info.Options.NoFields != def.NoFields {
				changes = append(changes, "different option NOFIELDS")
			}
			if info.Options.NoOffsets != def.NoOffsets {
				changes = append(changes, "different option NOOFFSETS")
			}
			if info.Options.MaxTextFields != def.MaxTextFields {
				changes = append(changes, "different option MAXTEXTFIELDS")
			}
			defaultScore := def.DefaultScore
			if defaultScore == 0 {
				defaultScore = 1
			}
			if info.Definition.DefaultScore != defaultScore {
				changes = append(changes, "different default score")
			}
		MAIN:
			for _, defField := range def.Fields {
				for _, infoField := range info.Fields {
					if defField.Name == infoField.Name {
						if defField.Type != infoField.Type {
							changes = append(changes, "different field type "+infoField.Name)
						} else {
							if defField.Type == redisSearchIndexFieldText {
								if defField.NoStem != infoField.NoStem {
									changes = append(changes, "different field nostem "+infoField.Name)
								}
								if defField.Weight != infoField.Weight {
									changes = append(changes, "different field weight "+infoField.Name)
								}
							} else if defField.Type == redisSearchIndexFieldTAG {
								if defField.TagSeparator != infoField.TagSeparator {
									changes = append(changes, "different field separator "+infoField.Name)
								}
							}
						}
						if defField.Sortable != infoField.Sortable {
							changes = append(changes, "different field sortable "+infoField.Name)
						}
						if defField.NoIndex != infoField.NoIndex {
							changes = append(changes, "different field noindex "+infoField.Name)
						}
						continue MAIN
					}
				}
				changes = append(changes, "new field "+defField.Name)
			}
		MAIN2:
			for _, infoField := range info.Fields {
				for _, defField := range def.Fields {
					if defField.Name == infoField.Name {
						continue MAIN2
					}
				}
				changes = append(changes, "unneeded field "+infoField.Name)
			}

			if len(changes) > 0 {
				alters = append(alters, search.addAlter(def, info.NumDocs, changes))
			}
		}
		for name, index := range engine.registry.redisSearchIndexes[poolName] {
			_, has := inRedis[name]
			if has {
				continue
			}
			alters = append(alters, search.addAlter(index, 0, []string{"new index"}))
		}
	}
	return alters
}

func (r *RedisSearch) addAlter(index *RedisSearchIndex, documents uint64, changes []string) RedisSearchIndexAlter {
	query := fmt.Sprintf("%v", r.createIndexArgs(index, index.Name))[1:]
	query = query[0 : len(query)-1]
	alter := RedisSearchIndexAlter{Pool: r.redis.config.GetCode(), Name: index.Name, Query: query, Changes: changes, search: r}
	indexToAdd := index.Name
	alter.Execute = func() {
		alter.search.ForceReindex(indexToAdd)
	}
	alter.Documents = documents
	return alter
}

func (r *RedisSearch) fillLogFields(operation, query string, start *time.Time, err error) {
	fillLogFields(r.engine.queryLoggersRedis, r.redis.config.GetCode(), sourceRedis, operation, query, start, err)
}

func (q *RedisSearchQuery) cutDate(date time.Time) int64 {
	return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local).Unix()
}

func (q *RedisSearchQuery) cutDateTime(date time.Time) int64 {
	return time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 0, time.Local).Unix()
}
