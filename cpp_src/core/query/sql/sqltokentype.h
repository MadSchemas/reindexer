#pragma once

namespace reindexer {
enum SqlTokenType {
	Start = 0,
	SelectSqlToken,
	DeleteSqlToken,
	StartAfterExplain,
	SingleSelectFieldSqlToken,
	SelectFieldsListSqlToken,
	AggregationSqlToken,
	FromSqlToken,
	NamespaceSqlToken,
	SelectConditionsStart,
	WhereSqlToken,
	WhereFieldSqlToken,
	ConditionSqlToken,
	OpSqlToken,
	WhereOpSqlToken,
	FieldNameSqlToken,
	WhereFieldValueSqlToken,
	WhereFieldNegateValueSqlToken,
	NullSqlToken,
	EmptySqlToken,
	NotSqlToken,
	AndSqlToken,
	OrSqlToken,
	BySqlToken,
	AllFieldsToken,
	SortDirectionSqlToken,
	FieldSqlToken,
	LeftSqlToken,
	InnerSqlToken,
	JoinSqlToken,
	MergeSqlToken,
	OnSqlToken,
	JoinedFieldNameSqlToken,
	DeleteConditionsStart,
	SetSqlToken,
	UpdateOptionsSqlToken,
	EqualPositionSqlToken,
	JoinTypesSqlToken,
	ST_DWithinSqlToken,
	ST_GeomFromTextSqlToken,
	GeomFieldSqlToken,
	StartAfterLocal,
	StartAfterLocalExplain,
};
}
