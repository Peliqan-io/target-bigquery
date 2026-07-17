from collections import OrderedDict
from typing import List

import pytest
import singer_sdk.typing as th
from google.cloud.bigquery import SchemaField

from unittest.mock import MagicMock, patch

from target_bigquery.core import SchemaTranslator, bigquery_type, transform_column_name, BigQueryTable, IngestionStrategy
from target_bigquery.proto_gen import proto_schema_factory_v2


@pytest.mark.parametrize(
    "name,rules,expected",
    [
        ("TestColumn", {"snake_case": True}, "test_column"),
        ("_TestColumn", {"snake_case": True}, "_test_column"),
        ("RANDOMCapsAREHere", {"snake_case": True}, "random_caps_are_here"),
        ("ALLCAPS", {"snake_case": True}, "allcaps"),
        ("ALL_CAPS", {"snake_case": True}, "all_caps"),
        ("SalesforceThing__c", {"snake_case": True}, "salesforce_thing__c"),
        ("TestColumn", {"lower": True}, "testcolumn"),
        ("TestColumn", {}, "TestColumn"),
        ("`TestColumn`", {}, "`TestColumn`"),
        ("ValidColumn", {"add_underscore_when_invalid": True}, "ValidColumn"),
        (
            "123InvalidColumn",
            {"add_underscore_when_invalid": True},
            "_123InvalidColumn",
        ),
        ("Shakespeare", {"quote": True}, "`Shakespeare`"),
        ("`Shakespeare`", {"quote": True}, "`Shakespeare`"),
        ("`ANewWorld`", {"snake_case": True}, "`a_new_world`"),
        (
            "123ANewWorld",
            {"snake_case": True, "add_underscore_when_invalid": True},
            "_123_a_new_world",
        ),
        (
            "`123ANewWorld`",
            {"snake_case": True, "add_underscore_when_invalid": True},
            "`_123_a_new_world`",
        ),
        (
            "123ANewWorld",
            {
                "snake_case": True,
                "lower": True,
                "add_underscore_when_invalid": True,
                "quote": True,
            },
            "`_123_a_new_world`",
        ),
    ],
    ids=[
        "snake_case",
        "snake_case_with_underscore_prefix",
        "snake_case_with_intermitten_caps",
        "snake_case_all_caps",
        "snake_case_all_caps_with_underscore",
        "snake_case_double_underscore",
        "lowercase",
        "no_rules_supplied",
        "no_rules_supplied_quoted_string",
        "add_underscore_rule_on_valid_column",
        "add_underscore_rule_on_invalid_column",
        "add_quotes",
        "add_quotes_on_quoted_string",
        "snake_case_quoted_string",
        "composite_rule_snake_case_underscore",
        "composite_rule_snake_case_underscore_on_quoted_column",
        "all_rules",
    ],
)
def test_transform_column_name(name: str, rules: dict, expected: str):
    assert transform_column_name(name, **rules) == expected


@pytest.mark.parametrize(
    "jsonschema_type,jsonschema_format,expected",
    [
        ("number", None, "float"),
        ("string", "date-time", "timestamp"),
        ("number", "time", "time"),
    ],
    ids=[
        "number_to_float",
        "datetime_format_in_jsonschema",
        "time_format_in_jsonschema",
    ],
)
def test_bigquery_type(jsonschema_type: str, jsonschema_format: str, expected: str):
    assert bigquery_type(jsonschema_type, jsonschema_format) == expected


@pytest.mark.parametrize(
    "schema,table,transforms,expected",
    [
        (
            {"type": "object", "properties": {"int_col_1": {"type": "integer"}}},
            BigQueryTable(name="table", dataset="some", project="project", jsonschema={}, ingestion_strategy=IngestionStrategy.FIXED),
            {},
            """CREATE OR REPLACE VIEW `project`.`some`.`table_view` AS 
SELECT 
    CAST(JSON_VALUE(data, '$.int_col_1') as INT64) as int_col_1,
 FROM `project`.`some`.`table`""",
        ),
        (
            {"type": "object", "properties": {"IntCol1": {"type": "integer"}}},
            BigQueryTable(name="table", dataset="some", project="project", jsonschema={}, ingestion_strategy=IngestionStrategy.FIXED),
            {"snake_case": True},
            """CREATE OR REPLACE VIEW `project`.`some`.`table_view` AS 
SELECT 
    CAST(JSON_VALUE(data, '$.IntCol1') as INT64) as int_col1,
 FROM `project`.`some`.`table`""",
        ),
        (
            th.PropertiesList(
                th.Property("id", th.StringType),
                th.Property("companyId", th.IntegerType),
                th.Property("email", th.StringType),
                th.Property("fullName", th.StringType),
                th.Property("firstName", th.StringType),
                th.Property("surname", th.StringType),
                th.Property("displayName", th.StringType),
                th.Property("creationDateTime", th.DateTimeType),
                th.Property(
                    "internal",
                    th.ObjectType(
                        th.Property("yearsSinceTermination", th.NumberType),
                        th.Property("terminationReason", th.StringType),
                        th.Property("probationEndDate", th.StringType),
                        th.Property("currentActiveStatusStartDate", th.StringType),
                        th.Property("terminationDate", th.StringType),
                        th.Property("status", th.StringType),
                        th.Property("terminationType", th.StringType),
                        th.Property("lifecycleStatus", th.StringType),
                    ),
                ),
                th.Property(
                    "work",
                    th.ObjectType(
                        th.Property(
                            "durationofemployment",
                            th.ObjectType(
                                th.Property("periodiso", th.StringType),
                                th.Property("sortfactor", th.IntegerType),
                                th.Property("humanize", th.StringType),
                            ),
                        ),
                        th.Property("startdate", th.StringType),
                        th.Property("manager", th.StringType),
                        th.Property("reportstoidincompany", th.IntegerType),
                        th.Property("employeeIdInCompany", th.IntegerType),
                        th.Property("shortstartdate", th.StringType),
                        th.Property("daysofpreviousservice", th.IntegerType),
                        th.Property(
                            "customColumns",
                            th.ObjectType(
                                th.Property("column_1655996461265", th.StringType),
                                th.Property("column_1644862416222", th.ArrayType(th.StringType)),
                                th.Property("column_1644861659664", th.ArrayType(th.StringType)),
                            ),
                        ),
                        th.Property(
                            "custom",
                            th.ObjectType(
                                th.Property("field_1651169416679", th.StringType),
                            ),
                        ),
                        th.Property("directreports", th.IntegerType),
                        th.Property("indirectreports", th.IntegerType),
                        th.Property("tenureyears", th.IntegerType),
                        th.Property("yearsofservice__it", th.IntegerType),
                        th.Property("tenuredurationyears", th.NumberType),
                        th.Property("tenuredurationyears_it", th.IntegerType),
                        th.Property(
                            "tenureduration",
                            th.ObjectType(
                                th.Property("periodiso", th.StringType),
                                th.Property("sortfactor", th.IntegerType),
                                th.Property("humanize", th.StringType),
                            ),
                        ),
                        th.Property(
                            "reportsTo",
                            th.ObjectType(
                                th.Property("id", th.StringType),
                                th.Property("email", th.StringType),
                                th.Property("firstName", th.StringType),
                                th.Property("surname", th.StringType),
                                th.Property("displayName", th.StringType),
                            ),
                        ),
                        th.Property("department", th.StringType),
                        th.Property("siteId", th.IntegerType),
                        th.Property("isManager", th.BooleanType),
                        th.Property("title", th.StringType),
                        th.Property("site", th.StringType),
                        th.Property("activeEffectiveDate", th.StringType),
                        th.Property("yearsofservice", th.NumberType),
                        th.Property("secondlevelmanager", th.NumberType),
                    ),
                ),
                th.Property(
                    "humanReadable",
                    th.ObjectType(
                        th.Property("id", th.StringType),
                        th.Property("companyId", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("fullName", th.StringType),
                        th.Property("firstName", th.StringType),
                        th.Property("surname", th.StringType),
                        th.Property("displayName", th.StringType),
                        th.Property("creationDateTime", th.StringType),
                        th.Property("avatarurl", th.StringType),
                        th.Property("secondname", th.StringType),
                        th.Property(
                            "work",
                            th.ObjectType(
                                th.Property("startdate", th.StringType),
                                th.Property("shortstartdate", th.StringType),
                                th.Property("manager", th.StringType),
                                th.Property("reportsToIdInComany", th.IntegerType),
                                th.Property("employeeIdInCompany", th.StringType),
                                th.Property("reportsTo", th.StringType),
                                th.Property("department", th.StringType),
                                th.Property("siteId", th.StringType),
                                th.Property("isManager", th.StringType),
                                th.Property("title", th.StringType),
                                th.Property("site", th.StringType),
                                th.Property("durationofemployment", th.StringType),
                                th.Property("daysofpreviousservice", th.StringType),
                                th.Property("directreports", th.StringType),
                                th.Property("tenureduration", th.StringType),
                                th.Property("activeeffectivedate", th.StringType),
                                th.Property("tenuredurationyears", th.StringType),
                                th.Property("yearsofservice", th.StringType),
                                th.Property("secondlevelmanager", th.StringType),
                                th.Property("indirectreports", th.StringType),
                                th.Property("tenureyears", th.StringType),
                                th.Property(
                                    "customColumns",
                                    th.ObjectType(
                                        th.Property("column_1664478354663", th.StringType),
                                        th.Property("column_1655996461265", th.StringType),
                                        th.Property("column_1644862416222", th.StringType),
                                        th.Property("column_1644861659664", th.StringType),
                                    ),
                                ),
                                th.Property(
                                    "custom",
                                    th.ObjectType(
                                        th.Property("field_1651169416679", th.StringType),
                                    ),
                                ),
                            ),
                        ),
                        th.Property(
                            "internal",
                            th.ObjectType(
                                th.Property("periodSinceTermination", th.StringType),
                                th.Property("yearsSinceTermination", th.StringType),
                                th.Property("terminationReason", th.StringType),
                                th.Property("probationEndDate", th.StringType),
                                th.Property("currentActiveStatusStartDate", th.StringType),
                                th.Property("terminationDate", th.StringType),
                                th.Property("status", th.StringType),
                                th.Property("terminationType", th.StringType),
                                th.Property("notice", th.StringType),
                                th.Property("lifecycleStatus", th.StringType),
                            ),
                        ),
                        th.Property(
                            "about",
                            th.ObjectType(
                                th.Property("superpowers", th.StringType),
                                th.Property("hobbies", th.StringType),
                                th.Property("avatar", th.StringType),
                                th.Property("about", th.StringType),
                                th.Property(
                                    "socialdata",
                                    th.ObjectType(
                                        th.Property("linkedin", th.StringType),
                                        th.Property("facebook", th.StringType),
                                        th.Property("twitter", th.StringType),
                                    ),
                                ),
                                th.Property(
                                    "custom",
                                    th.ObjectType(
                                        th.Property("field_1645133202751", th.StringType),
                                    ),
                                ),
                            ),
                        ),
                        th.Property(
                            "personal",
                            th.ObjectType(
                                th.Property("shortbirthdate", th.StringType),
                                th.Property("pronouns", th.StringType),
                                th.Property(
                                    "custom",
                                    th.ObjectType(
                                        th.Property("field_1647463606890", th.StringType),
                                        th.Property("field_1647619490812", th.StringType),
                                    ),
                                ),
                            ),
                        ),
                        th.Property(
                            "lifecycle",
                            th.ObjectType(
                                th.Property(
                                    "custom",
                                    th.ObjectType(
                                        th.Property("field_1651694080083", th.StringType),
                                    ),
                                ),
                            ),
                        ),
                        th.Property(
                            "payroll",
                            th.ObjectType(
                                th.Property(
                                    "employment",
                                    th.ObjectType(
                                        th.Property("siteWorkinPattern", th.StringType),
                                        th.Property("salaryPayType", th.StringType),
                                        th.Property("actualWorkingPattern", th.StringType),
                                        th.Property("activeeffectivedate", th.StringType),
                                        th.Property("workingPattern", th.StringType),
                                        th.Property("fte", th.StringType),
                                        th.Property("type", th.StringType),
                                        th.Property("contract", th.StringType),
                                        th.Property("calendarId", th.StringType),
                                        th.Property("weeklyHours", th.StringType),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ).to_dict(),
            BigQueryTable(name="totoro", dataset="neighbor", project="my", jsonschema={}, ingestion_strategy=IngestionStrategy.FIXED),
            {},
            """CREATE OR REPLACE VIEW `my`.`neighbor`.`totoro_view` AS 
SELECT 
    JSON_VALUE(data, '$.id') as id,
    CAST(JSON_VALUE(data, '$.companyId') as INT64) as companyId,
    JSON_VALUE(data, '$.email') as email,
    JSON_VALUE(data, '$.fullName') as fullName,
    JSON_VALUE(data, '$.firstName') as firstName,
    JSON_VALUE(data, '$.surname') as surname,
    JSON_VALUE(data, '$.displayName') as displayName,
    CAST(JSON_VALUE(data, '$.creationDateTime') as TIMESTAMP) as creationDateTime,
    STRUCT(
      CAST(JSON_VALUE(data, '$.internal.yearsSinceTermination') as FLOAT64) as yearsSinceTermination,
      JSON_VALUE(data, '$.internal.terminationReason') as terminationReason,
      JSON_VALUE(data, '$.internal.probationEndDate') as probationEndDate,
      JSON_VALUE(data, '$.internal.currentActiveStatusStartDate') as currentActiveStatusStartDate,
      JSON_VALUE(data, '$.internal.terminationDate') as terminationDate,
      JSON_VALUE(data, '$.internal.status') as status,
      JSON_VALUE(data, '$.internal.terminationType') as terminationType,
      JSON_VALUE(data, '$.internal.lifecycleStatus') as lifecycleStatus
    ) as internal,
    STRUCT(
      STRUCT(
        JSON_VALUE(data, '$.work.durationofemployment.periodiso') as periodiso,
        CAST(JSON_VALUE(data, '$.work.durationofemployment.sortfactor') as INT64) as sortfactor,
        JSON_VALUE(data, '$.work.durationofemployment.humanize') as humanize
      ) as durationofemployment,
      JSON_VALUE(data, '$.work.startdate') as startdate,
      JSON_VALUE(data, '$.work.manager') as manager,
      CAST(JSON_VALUE(data, '$.work.reportstoidincompany') as INT64) as reportstoidincompany,
      CAST(JSON_VALUE(data, '$.work.employeeIdInCompany') as INT64) as employeeIdInCompany,
      JSON_VALUE(data, '$.work.shortstartdate') as shortstartdate,
      CAST(JSON_VALUE(data, '$.work.daysofpreviousservice') as INT64) as daysofpreviousservice,
      STRUCT(
        JSON_VALUE(data, '$.work.customColumns.column_1655996461265') as column_1655996461265,
        ARRAY(
          SELECT   STRING(column_1644862416222__rows.column_1644862416222) as column_1644862416222
          FROM UNNEST(
              JSON_QUERY_ARRAY(data, '$.work.customColumns.column_1644862416222')
          ) AS column_1644862416222__rows
          WHERE   STRING(column_1644862416222__rows.column_1644862416222) IS NOT NULL
        ) AS column_1644862416222,
        ARRAY(
          SELECT   STRING(column_1644861659664__rows.column_1644861659664) as column_1644861659664
          FROM UNNEST(
              JSON_QUERY_ARRAY(data, '$.work.customColumns.column_1644861659664')
          ) AS column_1644861659664__rows
          WHERE   STRING(column_1644861659664__rows.column_1644861659664) IS NOT NULL
        ) AS column_1644861659664
      ) as customColumns,
      STRUCT(
        JSON_VALUE(data, '$.work.custom.field_1651169416679') as field_1651169416679
      ) as custom,
      CAST(JSON_VALUE(data, '$.work.directreports') as INT64) as directreports,
      CAST(JSON_VALUE(data, '$.work.indirectreports') as INT64) as indirectreports,
      CAST(JSON_VALUE(data, '$.work.tenureyears') as INT64) as tenureyears,
      CAST(JSON_VALUE(data, '$.work.yearsofservice__it') as INT64) as yearsofservice__it,
      CAST(JSON_VALUE(data, '$.work.tenuredurationyears') as FLOAT64) as tenuredurationyears,
      CAST(JSON_VALUE(data, '$.work.tenuredurationyears_it') as INT64) as tenuredurationyears_it,
      STRUCT(
        JSON_VALUE(data, '$.work.tenureduration.periodiso') as periodiso,
        CAST(JSON_VALUE(data, '$.work.tenureduration.sortfactor') as INT64) as sortfactor,
        JSON_VALUE(data, '$.work.tenureduration.humanize') as humanize
      ) as tenureduration,
      STRUCT(
        JSON_VALUE(data, '$.work.reportsTo.id') as id,
        JSON_VALUE(data, '$.work.reportsTo.email') as email,
        JSON_VALUE(data, '$.work.reportsTo.firstName') as firstName,
        JSON_VALUE(data, '$.work.reportsTo.surname') as surname,
        JSON_VALUE(data, '$.work.reportsTo.displayName') as displayName
      ) as reportsTo,
      JSON_VALUE(data, '$.work.department') as department,
      CAST(JSON_VALUE(data, '$.work.siteId') as INT64) as siteId,
      CAST(JSON_VALUE(data, '$.work.isManager') as BOOLEAN) as isManager,
      JSON_VALUE(data, '$.work.title') as title,
      JSON_VALUE(data, '$.work.site') as site,
      JSON_VALUE(data, '$.work.activeEffectiveDate') as activeEffectiveDate,
      CAST(JSON_VALUE(data, '$.work.yearsofservice') as FLOAT64) as yearsofservice,
      CAST(JSON_VALUE(data, '$.work.secondlevelmanager') as FLOAT64) as secondlevelmanager
    ) as work,
    STRUCT(
      JSON_VALUE(data, '$.humanReadable.id') as id,
      JSON_VALUE(data, '$.humanReadable.companyId') as companyId,
      JSON_VALUE(data, '$.humanReadable.email') as email,
      JSON_VALUE(data, '$.humanReadable.fullName') as fullName,
      JSON_VALUE(data, '$.humanReadable.firstName') as firstName,
      JSON_VALUE(data, '$.humanReadable.surname') as surname,
      JSON_VALUE(data, '$.humanReadable.displayName') as displayName,
      JSON_VALUE(data, '$.humanReadable.creationDateTime') as creationDateTime,
      JSON_VALUE(data, '$.humanReadable.avatarurl') as avatarurl,
      JSON_VALUE(data, '$.humanReadable.secondname') as secondname,
      STRUCT(
        JSON_VALUE(data, '$.humanReadable.work.startdate') as startdate,
        JSON_VALUE(data, '$.humanReadable.work.shortstartdate') as shortstartdate,
        JSON_VALUE(data, '$.humanReadable.work.manager') as manager,
        CAST(JSON_VALUE(data, '$.humanReadable.work.reportsToIdInComany') as INT64) as reportsToIdInComany,
        JSON_VALUE(data, '$.humanReadable.work.employeeIdInCompany') as employeeIdInCompany,
        JSON_VALUE(data, '$.humanReadable.work.reportsTo') as reportsTo,
        JSON_VALUE(data, '$.humanReadable.work.department') as department,
        JSON_VALUE(data, '$.humanReadable.work.siteId') as siteId,
        JSON_VALUE(data, '$.humanReadable.work.isManager') as isManager,
        JSON_VALUE(data, '$.humanReadable.work.title') as title,
        JSON_VALUE(data, '$.humanReadable.work.site') as site,
        JSON_VALUE(data, '$.humanReadable.work.durationofemployment') as durationofemployment,
        JSON_VALUE(data, '$.humanReadable.work.daysofpreviousservice') as daysofpreviousservice,
        JSON_VALUE(data, '$.humanReadable.work.directreports') as directreports,
        JSON_VALUE(data, '$.humanReadable.work.tenureduration') as tenureduration,
        JSON_VALUE(data, '$.humanReadable.work.activeeffectivedate') as activeeffectivedate,
        JSON_VALUE(data, '$.humanReadable.work.tenuredurationyears') as tenuredurationyears,
        JSON_VALUE(data, '$.humanReadable.work.yearsofservice') as yearsofservice,
        JSON_VALUE(data, '$.humanReadable.work.secondlevelmanager') as secondlevelmanager,
        JSON_VALUE(data, '$.humanReadable.work.indirectreports') as indirectreports,
        JSON_VALUE(data, '$.humanReadable.work.tenureyears') as tenureyears,
        STRUCT(
          JSON_VALUE(data, '$.humanReadable.work.customColumns.column_1664478354663') as column_1664478354663,
          JSON_VALUE(data, '$.humanReadable.work.customColumns.column_1655996461265') as column_1655996461265,
          JSON_VALUE(data, '$.humanReadable.work.customColumns.column_1644862416222') as column_1644862416222,
          JSON_VALUE(data, '$.humanReadable.work.customColumns.column_1644861659664') as column_1644861659664
        ) as customColumns,
        STRUCT(
          JSON_VALUE(data, '$.humanReadable.work.custom.field_1651169416679') as field_1651169416679
        ) as custom
      ) as work,
      STRUCT(
        JSON_VALUE(data, '$.humanReadable.internal.periodSinceTermination') as periodSinceTermination,
        JSON_VALUE(data, '$.humanReadable.internal.yearsSinceTermination') as yearsSinceTermination,
        JSON_VALUE(data, '$.humanReadable.internal.terminationReason') as terminationReason,
        JSON_VALUE(data, '$.humanReadable.internal.probationEndDate') as probationEndDate,
        JSON_VALUE(data, '$.humanReadable.internal.currentActiveStatusStartDate') as currentActiveStatusStartDate,
        JSON_VALUE(data, '$.humanReadable.internal.terminationDate') as terminationDate,
        JSON_VALUE(data, '$.humanReadable.internal.status') as status,
        JSON_VALUE(data, '$.humanReadable.internal.terminationType') as terminationType,
        JSON_VALUE(data, '$.humanReadable.internal.notice') as notice,
        JSON_VALUE(data, '$.humanReadable.internal.lifecycleStatus') as lifecycleStatus
      ) as internal,
      STRUCT(
        JSON_VALUE(data, '$.humanReadable.about.superpowers') as superpowers,
        JSON_VALUE(data, '$.humanReadable.about.hobbies') as hobbies,
        JSON_VALUE(data, '$.humanReadable.about.avatar') as avatar,
        JSON_VALUE(data, '$.humanReadable.about.about') as about,
        STRUCT(
          JSON_VALUE(data, '$.humanReadable.about.socialdata.linkedin') as linkedin,
          JSON_VALUE(data, '$.humanReadable.about.socialdata.facebook') as facebook,
          JSON_VALUE(data, '$.humanReadable.about.socialdata.twitter') as twitter
        ) as socialdata,
        STRUCT(
          JSON_VALUE(data, '$.humanReadable.about.custom.field_1645133202751') as field_1645133202751
        ) as custom
      ) as about,
      STRUCT(
        JSON_VALUE(data, '$.humanReadable.personal.shortbirthdate') as shortbirthdate,
        JSON_VALUE(data, '$.humanReadable.personal.pronouns') as pronouns,
        STRUCT(
          JSON_VALUE(data, '$.humanReadable.personal.custom.field_1647463606890') as field_1647463606890,
          JSON_VALUE(data, '$.humanReadable.personal.custom.field_1647619490812') as field_1647619490812
        ) as custom
      ) as personal,
      STRUCT(
        STRUCT(
          JSON_VALUE(data, '$.humanReadable.lifecycle.custom.field_1651694080083') as field_1651694080083
        ) as custom
      ) as lifecycle,
      STRUCT(
        STRUCT(
          JSON_VALUE(data, '$.humanReadable.payroll.employment.siteWorkinPattern') as siteWorkinPattern,
          JSON_VALUE(data, '$.humanReadable.payroll.employment.salaryPayType') as salaryPayType,
          JSON_VALUE(data, '$.humanReadable.payroll.employment.actualWorkingPattern') as actualWorkingPattern,
          JSON_VALUE(data, '$.humanReadable.payroll.employment.activeeffectivedate') as activeeffectivedate,
          JSON_VALUE(data, '$.humanReadable.payroll.employment.workingPattern') as workingPattern,
          JSON_VALUE(data, '$.humanReadable.payroll.employment.fte') as fte,
          JSON_VALUE(data, '$.humanReadable.payroll.employment.type') as type,
          JSON_VALUE(data, '$.humanReadable.payroll.employment.contract') as contract,
          JSON_VALUE(data, '$.humanReadable.payroll.employment.calendarId') as calendarId,
          JSON_VALUE(data, '$.humanReadable.payroll.employment.weeklyHours') as weeklyHours
        ) as employment
      ) as payroll
    ) as humanReadable,
 FROM `my`.`neighbor`.`totoro`""",
        ),
    ],
    ids=[
        "generate_basic_view_stmt",
        "generate_basic_view_with_column_name_transforms",
        "generate_convoluted_view",
    ],
)
def test_schema_translator_views(schema: dict, table: BigQueryTable, transforms: dict, expected: str):
    assert (
        SchemaTranslator(
            schema,
            transforms,
        ).generate_view_statement(table)
        == expected
    )


@pytest.mark.parametrize(
    "schema,transforms,expected",
    [
        (
            {"type": "object", "properties": {"IntColumn": {"type": "integer"}}},
            {},
            [SchemaField("IntColumn", "integer")],
        ),
        (
            {"type": "object", "properties": {"IntColumn": {"type": "integer"}}},
            {"snake_case": True},
            [SchemaField("int_column", "integer")],
        ),
    ],
    ids=["basic_schema_translation", "schema_translation_with_transform"],
)
def test_schema_translator_tables(schema: dict, transforms: dict, expected: List[SchemaField]):
    assert (
        SchemaTranslator(
            schema,
            transforms,
        ).translated_schema_transformed
        == expected
    )


@pytest.mark.parametrize(
    "schema,transforms,records,expected",
    [
        (
            {"type": "object", "properties": {"IntColumn": {"type": "integer"}}},
            {},
            [{"IntColumn": 1}],
            [{"IntColumn": 1}],
        ),
        (
            {"type": "object", "properties": {"IntColumn": {"type": "integer"}}},
            {"snake_case": True},
            [{"IntColumn": 1}],
            [{"int_column": 1}],
        ),
        (
            {
                "type": "object",
                "properties": {
                    "NestedLevelOne": {
                        "type": "object",
                        "properties": {
                            "NestedLevelTwo": {
                                "type": "object",
                                "properties": {"IntColumn": {"type": "integer"}},
                            }
                        },
                    }
                },
            },
            {"snake_case": True},
            [{"NestedLevelOne": {"NestedLevelTwo": {"IntColumn": 1}}}],
            [{"nested_level_one": {"nested_level_two": {"int_column": 1}}}],
        ),
        (
            {
                "type": "object",
                "properties": {
                    "NestedLevelOne": {
                        "type": "object",
                        "properties": {
                            "NestedLevelTwo": {
                                "type": "object",
                                "properties": {
                                    "ArrayColumn": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {"IntColumn": {"type": "integer"}},
                                        },
                                    }
                                },
                            }
                        },
                    }
                },
            },
            {"snake_case": True},
            [
                {"NestedLevelOne": {"NestedLevelTwo": {"ArrayColumn": [{"IntColumn": 1}]}}},
                {
                    "NestedLevelOne": {
                        "NestedLevelTwo": {
                            "ArrayColumn": [
                                {"IntColumn": 1},
                                {"IntColumn": 2},
                                {"IntColumn": 3},
                            ]
                        }
                    }
                },
            ],
            [
                {"nested_level_one": {"nested_level_two": {"array_column": [{"int_column": 1}]}}},
                {
                    "nested_level_one": {
                        "nested_level_two": {
                            "array_column": [
                                {"int_column": 1},
                                {"int_column": 2},
                                {"int_column": 3},
                            ]
                        }
                    }
                },
            ],
        ),
    ],
    ids=[
        "record_translation_noop",
        "record_translation_with_transform",
        "record_translation_nested_with_transform",
        "record_translation_nested_list_with_transform",
    ],
)
def test_schema_translator_records(
    schema: dict, transforms: dict, records: List[dict], expected: List[dict]
):
    assert [
        SchemaTranslator(
            schema,
            transforms,
        ).translate_record(record)
        for record in records
    ] == expected


def test_jit_compile_proto():
    jit = proto_schema_factory_v2(
        [
            SchemaField("IntColumn", "integer"),
            SchemaField("StringColumn", "string"),
            SchemaField("FloatColumn", "float"),
            SchemaField("BooleanColumn", "boolean"),
            SchemaField("TimestampColumn", "timestamp"),
            SchemaField("DateColumn", "date"),
            SchemaField("TimeColumn", "time"),
        ],
    )
    payload = {
        "IntColumn": 1,
        "StringColumn": "test",
        "FloatColumn": 1.0,
        "BooleanColumn": True,
        "TimestampColumn": "2020-01-01",
        "DateColumn": "2020-01-01",
        "TimeColumn": "00:00:00",
    }
    data = jit()
    descript = jit.DESCRIPTOR
    for f in descript.fields:
        if f.name in payload:
            setattr(data, f.name, payload[f.name])
    assert (
        data.SerializeToString()
        == b"\x08\x01\x12\x04test\x19\x00\x00\x00\x00\x00\x00\xf0?"
        b" \x01*\n2020-01-012\n2020-01-01:\x0800:00:00"
    )


# --------------------------------------------------------------------------- #
# delete-sync (DELETERECORD)                                                    #
# --------------------------------------------------------------------------- #


def _bare_target(config=None, buffer=None):
    """A TargetBigQuery without __init__ (no worker pool / no BQ client)."""
    from target_bigquery.target import TargetBigQuery

    t = TargetBigQuery.__new__(TargetBigQuery)
    t._delete_buffer = {} if buffer is None else buffer
    t._config = config or {"project": "p", "dataset": "d"}
    t._credentials = MagicMock()
    return t


def test_process_unknown_message_buffers_deleterecord():
    t = _bare_target()
    t._process_unknown_message(
        {"type": "DELETERECORD", "stream": "Accounts", "record": {"id": "a1"}}
    )
    t._process_unknown_message(
        {"type": "DELETERECORD", "stream": "Accounts", "record": {"id": "a2"}}
    )
    t._process_unknown_message(
        {"type": "DELETERECORD", "stream": "Contacts", "record": {"id": "c1"}}
    )
    assert t._delete_buffer == {
        "Accounts": [{"id": "a1"}, {"id": "a2"}],
        "Contacts": [{"id": "c1"}],
    }


def test_process_unknown_message_raises_for_other_types():
    t = _bare_target()
    with pytest.raises(ValueError):
        t._process_unknown_message({"type": "SOMETHING_ELSE"})
    assert t._delete_buffer == {}


def _run_flush(buffer, denormalized=False, extra_config=None, query_side_effect=None):
    config = {"project": "p", "dataset": "d", "denormalized": denormalized}
    if extra_config:
        config.update(extra_config)
    t = _bare_target(config=config, buffer=buffer)
    client = MagicMock()
    if query_side_effect is not None:
        client.query.side_effect = query_side_effect
    with patch("target_bigquery.target.bigquery_client_factory", return_value=client):
        t._flush_deletes()
    return t, client


def _sql(client):
    return client.query.call_args.args[0]


def _vals(client):
    return client.query.call_args.kwargs["job_config"].query_parameters[0].values


def test_flush_fixed_single_pk():
    t, client = _run_flush({"Accounts": [{"id": "a1"}, {"id": "a2"}]}, denormalized=False)
    assert client.query.call_count == 1
    assert _sql(client) == (
        "DELETE FROM `p`.`d`.`accounts` "
        "WHERE JSON_VALUE(data, '$.id') IN UNNEST(@vals)"
    )
    assert _vals(client) == ["a1", "a2"]
    assert t._delete_buffer == {}


def test_flush_fixed_composite_pk():
    _, client = _run_flush({"Journals": [{"id": "g1", "division": 100}]}, denormalized=False)
    assert _sql(client) == (
        "DELETE FROM `p`.`d`.`journals` WHERE "
        "CONCAT(JSON_VALUE(data, '$.id'), '\\u001f', JSON_VALUE(data, '$.division')) "
        "IN UNNEST(@vals)"
    )
    assert _vals(client) == ["g1\u001f100"]


def test_flush_denormalized_single_pk():
    _, client = _run_flush({"Accounts": [{"id": "a1"}]}, denormalized=True)
    assert _sql(client) == (
        "DELETE FROM `p`.`d`.`accounts` WHERE CAST(`id` AS STRING) IN UNNEST(@vals)"
    )
    assert _vals(client) == ["a1"]


def test_flush_denormalized_composite_pk():
    _, client = _run_flush({"Inv": [{"a": 1, "b": 2}]}, denormalized=True)
    assert _sql(client) == (
        "DELETE FROM `p`.`d`.`inv` WHERE "
        "CONCAT(CAST(`a` AS STRING), '\\u001f', CAST(`b` AS STRING)) IN UNNEST(@vals)"
    )
    assert _vals(client) == ["1\u001f2"]


def test_flush_denormalized_applies_column_transform():
    # baserow runs BigQuery denormalized with add_underscore_when_invalid, so a
    # digit-leading key is stored as a `_`-prefixed column; the delete must match.
    _, client = _run_flush(
        {"Accounts": [{"123id": "a1"}]},
        denormalized=True,
        extra_config={"column_name_transforms": {"add_underscore_when_invalid": True}},
    )
    assert _sql(client) == (
        "DELETE FROM `p`.`d`.`accounts` WHERE CAST(`_123id` AS STRING) IN UNNEST(@vals)"
    )
    assert _vals(client) == ["a1"]


def test_flush_chunks_by_batch_size():
    records = [{"id": f"a{i}"} for i in range(5)]
    _, client = _run_flush({"Accounts": records}, extra_config={"batch_size": 2})
    assert client.query.call_count == 3  # 2 + 2 + 1
    chunks = [
        c.kwargs["job_config"].query_parameters[0].values
        for c in client.query.call_args_list
    ]
    assert chunks == [["a0", "a1"], ["a2", "a3"], ["a4"]]


def test_flush_skips_missing_table_and_continues():
    # NotFound (table/dataset absent) is a best-effort skip; other streams still run.
    from google.api_core.exceptions import NotFound

    t = _bare_target(buffer={"Gone": [{"id": "x"}], "Accounts": [{"id": "a1"}]})
    client = MagicMock()
    client.query.side_effect = [NotFound("missing"), MagicMock()]
    with patch("target_bigquery.target.bigquery_client_factory", return_value=client):
        t._flush_deletes()  # must not raise
    assert client.query.call_count == 2  # second stream still attempted
    assert t._delete_buffer == {}  # cleared after a clean pass


def test_flush_raises_on_real_error_and_keeps_buffer():
    # A real failure (connection/credential/etc.) must propagate so state never
    # advances, and the buffer is NOT cleared.
    t = _bare_target(buffer={"Accounts": [{"id": "a1"}]})
    client = MagicMock()
    client.query.side_effect = Exception("connection reset")
    with patch("target_bigquery.target.bigquery_client_factory", return_value=client):
        with pytest.raises(Exception):
            t._flush_deletes()
    assert t._delete_buffer == {"Accounts": [{"id": "a1"}]}


def test_drain_all_does_not_write_state_when_delete_fails():
    # The bookmark must not advance past a delete that failed: a raising flush
    # aborts drain_all before _write_state_message.
    t = _bare_target(buffer={"Accounts": [{"id": "a1"}]})
    t._latest_state = {"bookmarks": {"x": 1}}
    t._sinks_active = {}
    t.workers = []
    t.max_parallelism = 1
    t._drain_all = MagicMock()
    t._write_state_message = MagicMock()
    t._reset_max_record_age = MagicMock()
    client = MagicMock()
    client.query.side_effect = Exception("creds error")
    with patch("target_bigquery.target.bigquery_client_factory", return_value=client):
        with pytest.raises(Exception):
            t.drain_all(is_endofpipe=True)
    t._write_state_message.assert_not_called()


def test_flush_one_query_per_stream():
    _, client = _run_flush(
        {"Accounts": [{"id": "a1"}], "Contacts": [{"id": "c1"}]}
    )
    assert client.query.call_count == 2
    sqls = " ".join(c.args[0] for c in client.query.call_args_list)
    assert "`p`.`d`.`accounts`" in sqls
    assert "`p`.`d`.`contacts`" in sqls


def test_flush_applies_prefix_and_sanitizes_table_name():
    _, client = _run_flush(
        {"My-Stream.X": [{"id": "a1"}]}, extra_config={"table_name_prefix": "px_"}
    )
    assert _sql(client).startswith("DELETE FROM `p`.`d`.`px_my_stream_x`")


def test_flush_skips_empty_record_lists():
    t, client = _run_flush({"Accounts": []})
    client.query.assert_not_called()
    assert t._delete_buffer == {}


# --------------------------------------------------------------------------- #
# checkpoint() — mid-run MERGE (PQ-3547)                                        #
# --------------------------------------------------------------------------- #
from copy import copy as _copy


def _merge_sink():
    """A BaseBigQuerySink-like object with merge staging, no __init__/BQ."""
    from target_bigquery.core import BaseBigQuerySink, BigQueryTable, IngestionStrategy

    # BaseBigQuerySink is abstract (process_batch/worker_cls_factory), so
    # object.__new__ refuses to instantiate it directly. A trivial concrete
    # subclass sidesteps that without changing any inherited behavior we
    # exercise here (checkpoint()/_new_staging_table() are defined on the
    # base class itself).
    class _ConcreteSink(BaseBigQuerySink):
        def process_batch(self, context):
            raise NotImplementedError

        @staticmethod
        def worker_cls_factory(worker_executor_cls, config):
            raise NotImplementedError

    s = _ConcreteSink.__new__(_ConcreteSink)
    s.client = MagicMock()
    s.stream_name = "SalesInvoiceLines"
    s._key_properties = ["id"]  # key_properties is a read-only property in the SDK base
    s._config = {"project": "p", "dataset": "d"}  # config is also a read-only property
    opts = {
        "project": "p", "dataset": "d", "jsonschema": {"properties": {}},
        "transforms": {}, "ingestion_strategy": IngestionStrategy.DENORMALIZED,
    }
    s._staging_opts = opts
    s._staging_seq = 0
    real = BigQueryTable(name="salesinvoicelines", **opts)
    s.merge_target = _copy(real)
    s.table = BigQueryTable(name="salesinvoicelines__100", **opts)
    # apply_transforms is a read-only property (derived from ingestion_strategy)
    # and is unused by checkpoint()/_merge_staging_into_target(); not set here.
    return s


def test_checkpoint_merges_and_rotates_temp_without_teardown(monkeypatch):
    s = _merge_sink()
    # stub dedupe/merge to avoid building real SQL/schema
    monkeypatch.setattr(type(s), "_is_dedupe_before_upsert_candidate", lambda self: False)
    created = []
    monkeypatch.setattr(type(s), "_new_staging_table",
                        lambda self: created.append(True))
    prev_target = s.merge_target
    s.checkpoint()
    assert s.client.query.called                 # a MERGE was issued
    assert created == [True]                      # fresh temp rotated in
    assert s.merge_target is prev_target          # merge_target NOT torn down


def test_checkpoint_raises_and_does_not_drop_on_merge_failure(monkeypatch):
    s = _merge_sink()
    monkeypatch.setattr(type(s), "_is_dedupe_before_upsert_candidate", lambda self: False)
    monkeypatch.setattr(type(s), "_new_staging_table", lambda self: None)
    s.client.query.side_effect = Exception("schema mismatch")
    with pytest.raises(Exception):
        s.checkpoint()
    # Exactly one query call was made (the failed MERGE attempt) and no
    # separate standalone DROP-only query was issued afterward. (The old
    # buggy code issued a second "DROP TABLE IF EXISTS ..." query in its
    # except clause; merge_sql itself legitimately ENDS with a DROP TABLE
    # statement, so asserting on substring "DROP TABLE" would be wrong here.)
    assert s.client.query.call_count == 1


def test_checkpoint_noop_when_no_merge_target(monkeypatch):
    s = _merge_sink()
    s.merge_target = None
    monkeypatch.setattr(type(s), "_new_staging_table",
                        lambda self: (_ for _ in ()).throw(AssertionError("rotated")))
    s.checkpoint()                                # must not raise / not rotate
    assert not s.client.query.called


# --------------------------------------------------------------------------- #
# checkpoint() barrier override for storage_write (PQ-3547)                    #
# --------------------------------------------------------------------------- #
def test_storage_write_checkpoint_barrier_before_merge(monkeypatch):
    """storage_write must commit streams + await fallback Load Jobs BEFORE the
    MERGE runs (order matters: rows must be durable in the temp first), and
    must refresh parent/template only AFTER the MERGE (rotation) succeeds."""
    from target_bigquery.storage_write import BigQueryStorageWriteDenormalizedSink as SW
    import target_bigquery.core as core

    # BigQueryStorageWriteDenormalizedSink is not abstract (unlike
    # BaseBigQuerySink), so __new__ works directly without a concrete subclass.
    s = SW.__new__(SW)
    calls = []
    s.commit_streams = lambda: calls.append("commit")
    s._wait_for_fallback_jobs = lambda: calls.append("fallback")
    s._refresh_write_destination = lambda: calls.append("refresh")
    # Base checkpoint() (Task 1) records "merge"; patched via monkeypatch so
    # it's restored automatically even if the assertion below fails.
    monkeypatch.setattr(core.BaseBigQuerySink, "checkpoint", lambda self: calls.append("merge"))

    SW.checkpoint(s)

    assert calls == ["commit", "fallback", "merge", "refresh"]


# --------------------------------------------------------------------------- #
# _refresh_write_destination() — generation rotation (PQ-3547 C1)              #
# --------------------------------------------------------------------------- #
def _sw_sink():
    """A BigQueryStorageWriteDenormalizedSink-like object with no BQ/network
    dependency, built the same way as `_merge_sink()` but with the storage_write
    write-destination attributes (`parent`/`template`) initialized via
    `_refresh_write_destination()`, mirroring what `__init__` now does."""
    from target_bigquery.storage_write import BigQueryStorageWriteDenormalizedSink as SW
    from target_bigquery.core import BigQueryTable, IngestionStrategy
    from target_bigquery.proto_gen import proto_schema_factory_v2

    s = SW.__new__(SW)
    s.client = MagicMock()
    s.stream_name = "SalesInvoiceLines"
    s._key_properties = ["id"]  # key_properties is a read-only property in the SDK base
    s._config = {"project": "p", "dataset": "d"}  # config is also a read-only property
    opts = {
        "project": "p", "dataset": "d", "jsonschema": {"properties": {}},
        "transforms": {}, "ingestion_strategy": IngestionStrategy.DENORMALIZED,
    }
    s._staging_opts = opts
    s._staging_seq = 0
    real = BigQueryTable(name="salesinvoicelines", **opts)
    s.merge_target = _copy(real)
    s.table = BigQueryTable(name="salesinvoicelines__gen1_1", **opts)
    # Bypass the real schema-translation path (irrelevant to rotation logic);
    # a real proto message class is still used so generate_template's
    # descriptor-embedding logic runs unmocked.
    s._proto_schema = proto_schema_factory_v2([])
    s.open_streams = set()
    s._refresh_write_destination()  # what __init__ now does
    return s


def test_refresh_write_destination_parent_matches_current_table():
    from google.cloud.bigquery_storage_v1 import BigQueryWriteClient

    s = _sw_sink()
    assert s.parent == BigQueryWriteClient.table_path("p", "d", "salesinvoicelines__gen1_1")


def test_checkpoint_rotates_parent_and_template_to_generation_two(monkeypatch):
    """After a successful checkpoint (MERGE + staging-table rotation), `parent`
    must reference the NEW (generation-2) table and a fresh template object
    must have been created — never the dropped generation-1 table/template."""
    from google.cloud.bigquery_storage_v1 import BigQueryWriteClient
    from target_bigquery.core import BigQueryTable
    from target_bigquery.storage_write import BigQueryStorageWriteDenormalizedSink as SW
    import target_bigquery.core as core
    import target_bigquery.storage_write as sw

    s = _sw_sink()
    gen1_parent, gen1_template = s.parent, s.template
    assert gen1_parent == BigQueryWriteClient.table_path("p", "d", "salesinvoicelines__gen1_1")

    s.commit_streams = lambda: None
    s._wait_for_fallback_jobs = lambda: None

    def fake_super_checkpoint(self):
        # Mirrors what BaseBigQuerySink.checkpoint() does on a successful
        # MERGE: rotate self.table to a fresh staging table.
        self.table = BigQueryTable(name="salesinvoicelines__gen2_1", **self._staging_opts)

    monkeypatch.setattr(core.BaseBigQuerySink, "checkpoint", fake_super_checkpoint)
    template_calls = []
    real_generate_template = sw.generate_template
    monkeypatch.setattr(
        sw, "generate_template",
        lambda schema: (template_calls.append(schema), real_generate_template(schema))[1],
    )

    SW.checkpoint(s)

    expected_gen2_parent = BigQueryWriteClient.table_path("p", "d", "salesinvoicelines__gen2_1")
    assert s.parent == expected_gen2_parent
    assert s.parent != gen1_parent
    assert s.template is not gen1_template
    assert len(template_calls) == 1  # fresh template built exactly once, post-MERGE

    # A Job created after this checkpoint carries the NEW parent, never the
    # dropped generation-1 table.
    job = sw.Job(parent=s.parent, template=s.template, stream_notifier=None, data=None)
    assert job.parent == expected_gen2_parent
    assert job.parent != gen1_parent


def test_checkpoint_does_not_refresh_destination_when_merge_raises(monkeypatch):
    """If the MERGE (super().checkpoint()) raises, no rotation happened, so
    parent/template must be left pointing at the still-current staging table."""
    from target_bigquery.storage_write import BigQueryStorageWriteDenormalizedSink as SW
    import target_bigquery.core as core
    import target_bigquery.storage_write as sw

    s = _sw_sink()
    gen1_parent, gen1_template = s.parent, s.template

    s.commit_streams = lambda: None
    s._wait_for_fallback_jobs = lambda: None
    monkeypatch.setattr(
        core.BaseBigQuerySink, "checkpoint",
        lambda self: (_ for _ in ()).throw(Exception("merge failed")),
    )
    refresh_calls = []
    monkeypatch.setattr(
        sw.BigQueryStorageWriteSink, "_refresh_write_destination",
        lambda self: refresh_calls.append(True),
    )

    with pytest.raises(Exception, match="merge failed"):
        SW.checkpoint(s)

    assert refresh_calls == []          # never reached — merge raised first
    assert s.parent == gen1_parent      # unchanged: still generation 1
    assert s.template is gen1_template  # unchanged: same template object


class _FakeSink:
    """Minimal stand-in for a BaseBigQuerySink used only to carry
    merge_target/overwrite_target for scope-gating checks (§5.3). Not a
    MagicMock, since a MagicMock's un-configured attribute access returns a
    truthy Mock rather than None, which would defeat the `is not None`
    checks under test."""

    def __init__(self, merge_target=None, overwrite_target=None):
        self.merge_target = merge_target
        self.overwrite_target = overwrite_target


# --------------------------------------------------------------------------- #
# checkpoint_row_threshold config + record counter + STATE trigger (PQ-3547)   #
# --------------------------------------------------------------------------- #
def _counter_target(threshold, method="batch_job", sinks_active=None):
    from target_bigquery.target import TargetBigQuery

    t = TargetBigQuery.__new__(TargetBigQuery)
    t._config = {
        "project": "p",
        "dataset": "d",
        "checkpoint_row_threshold": threshold,
        "method": method,
    }
    t._records_since_checkpoint = 0
    t._checkpoint_row_threshold = threshold
    t._latest_state = {"bookmarks": {}}
    # Default: one eligible (upserting) sink, so threshold tests exercise only
    # the counter/threshold logic. Scope-gating tests override this.
    t._sinks_active = (
        sinks_active if sinks_active is not None else {"A": _FakeSink(merge_target=object())}
    )
    t.drain_all = MagicMock()
    return t


def test_state_message_triggers_checkpoint_when_threshold_met(monkeypatch):
    import target_bigquery.target as tgt

    # Target (TargetBigQuery.__mro__[1]) defines the SDK base
    # _process_state_message; stub it so our override's own logic (call
    # super() then evaluate the trigger) is what's under test.
    assert tgt.TargetBigQuery.__mro__[1].__name__ == "Target"
    monkeypatch.setattr(
        tgt.TargetBigQuery.__mro__[1], "_process_state_message", lambda self, m: None
    )
    t = _counter_target(threshold=3)
    t._records_since_checkpoint = 3
    t._process_state_message({"type": "STATE", "value": {"bookmarks": {"A": 1}}})
    t.drain_all.assert_called_once_with(is_endofpipe=False)


def test_state_message_no_checkpoint_below_threshold(monkeypatch):
    import target_bigquery.target as tgt

    monkeypatch.setattr(
        tgt.TargetBigQuery.__mro__[1], "_process_state_message", lambda self, m: None
    )
    t = _counter_target(threshold=1000)
    t._records_since_checkpoint = 10
    t._process_state_message({"type": "STATE", "value": {}})
    t.drain_all.assert_not_called()


def test_record_message_increments_counter(monkeypatch):
    import target_bigquery.target as tgt

    monkeypatch.setattr(
        tgt.TargetBigQuery.__mro__[1], "_process_record_message", lambda self, m: None
    )
    t = _counter_target(threshold=1000)
    t._process_record_message({"type": "RECORD", "stream": "A", "record": {}})
    assert t._records_since_checkpoint == 1


@pytest.mark.parametrize("threshold", [0, 1])
def test_threshold_zero_checkpoints_every_boundary_positive_coalesces(monkeypatch, threshold):
    import target_bigquery.target as tgt

    monkeypatch.setattr(
        tgt.TargetBigQuery.__mro__[1], "_process_state_message", lambda self, m: None
    )
    t = _counter_target(threshold=threshold)
    t._records_since_checkpoint = 0
    # threshold 0: 0 >= 0 -> every boundary checkpoints.
    # threshold 1: 0 >= 1 is False -> this boundary does NOT checkpoint yet.
    t._process_state_message({"type": "STATE", "value": {}})
    if threshold == 0:
        t.drain_all.assert_called_once_with(is_endofpipe=False)
    else:
        t.drain_all.assert_not_called()


# --------------------------------------------------------------------------- #
# row-threshold trigger scope gating (§5.3): only batch_job/storage_write_api #
# with an active upsert (merge_target) sink, and never with an overwrite     #
# sink, may take the mid-run row-threshold checkpoint path.                  #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("method", ["gcs_stage", "streaming_insert"])
def test_no_row_threshold_checkpoint_for_out_of_scope_methods(monkeypatch, method):
    import target_bigquery.target as tgt

    monkeypatch.setattr(
        tgt.TargetBigQuery.__mro__[1], "_process_state_message", lambda self, m: None
    )
    t = _counter_target(
        threshold=0,
        method=method,
        sinks_active={"A": _FakeSink(merge_target=object())},
    )
    t._records_since_checkpoint = 0
    t._process_state_message({"type": "STATE", "value": {}})
    t.drain_all.assert_not_called()


@pytest.mark.parametrize("method", ["batch_job", "storage_write_api"])
def test_no_row_threshold_checkpoint_when_a_sink_is_overwrite_mode(monkeypatch, method):
    import target_bigquery.target as tgt

    monkeypatch.setattr(
        tgt.TargetBigQuery.__mro__[1], "_process_state_message", lambda self, m: None
    )
    t = _counter_target(
        threshold=0,
        method=method,
        sinks_active={
            "A": _FakeSink(merge_target=object()),
            "B": _FakeSink(overwrite_target=object()),
        },
    )
    t._records_since_checkpoint = 0
    t._process_state_message({"type": "STATE", "value": {}})
    t.drain_all.assert_not_called()


@pytest.mark.parametrize("method", ["batch_job", "storage_write_api"])
def test_row_threshold_checkpoint_eligible_for_batch_job_and_storage_write_upsert(
    monkeypatch, method
):
    import target_bigquery.target as tgt

    monkeypatch.setattr(
        tgt.TargetBigQuery.__mro__[1], "_process_state_message", lambda self, m: None
    )
    t = _counter_target(
        threshold=0,
        method=method,
        sinks_active={"A": _FakeSink(merge_target=object())},
    )
    t._records_since_checkpoint = 0
    t._process_state_message({"type": "STATE", "value": {}})
    t.drain_all.assert_called_once_with(is_endofpipe=False)


@pytest.mark.parametrize("method", ["batch_job", "storage_write_api"])
def test_no_row_threshold_checkpoint_when_no_sink_is_upserting(monkeypatch, method):
    """Pure append-mode sinks (no merge_target, no overwrite_target) are not
    in scope either -- there is nothing for checkpoint()/MERGE to do."""
    import target_bigquery.target as tgt

    monkeypatch.setattr(
        tgt.TargetBigQuery.__mro__[1], "_process_state_message", lambda self, m: None
    )
    t = _counter_target(threshold=0, method=method, sinks_active={"A": _FakeSink()})
    t._records_since_checkpoint = 0
    t._process_state_message({"type": "STATE", "value": {}})
    t.drain_all.assert_not_called()


# --------------------------------------------------------------------------- #
# drain_all non-endofpipe path checkpoints (MERGE) + resets counter (PQ-3547) #
# --------------------------------------------------------------------------- #
def _checkpoint_mock_sink(merge_target=None, overwrite_target=None):
    """A MagicMock sink with *real* merge_target/overwrite_target values.

    A bare MagicMock's un-configured attribute is a truthy Mock (never None),
    which would defeat the `merge_target is not None` / `overwrite_target is
    None` scope checks the drain_all gate makes per sink. Setting them to real
    values (object() or None) lets the eligibility predicate behave."""
    s = MagicMock()
    s.merge_target = merge_target
    s.overwrite_target = overwrite_target
    return s


def test_drain_all_non_endofpipe_checkpoints_and_resets_counter():
    from target_bigquery.target import TargetBigQuery

    t = TargetBigQuery.__new__(TargetBigQuery)
    t._config = {"project": "p", "dataset": "d"}
    t._latest_state = {"bookmarks": {"A": 5}}
    sink = _checkpoint_mock_sink(merge_target=object())  # eligible: upsert sink
    t._sinks_active = {"A": sink}
    t.workers = []
    t.max_parallelism = 1
    t._delete_buffer = {}
    t._records_since_checkpoint = 42
    t._drain_all = MagicMock()
    t._raise_pending_worker_error = MagicMock()
    t._write_state_message = MagicMock()
    t._reset_max_record_age = MagicMock()
    t.drain_all(is_endofpipe=False)
    sink.checkpoint.assert_called_once()  # MERGE ran mid-run
    sink.clean_up.assert_not_called()  # not a teardown
    assert t._records_since_checkpoint == 0  # window reset
    t._write_state_message.assert_called_once()


# --------------------------------------------------------------------------- #
# fail-fast checkpoint finalization (PQ-3547 §5.2/§7): the first sink.checkpoint()
# / sink.clean_up() failure propagates immediately with no try/except around
# the sink loop. Earlier successful sinks in the same boundary are NOT rolled
# back, but no STATE is written for that boundary and later sinks are not
# finalized (fail-fast, not per-stream isolation).                            #
# --------------------------------------------------------------------------- #
def _failfast_target(sinks, is_endofpipe):
    from target_bigquery.target import TargetBigQuery

    t = TargetBigQuery.__new__(TargetBigQuery)
    t._config = {"project": "p", "dataset": "d"}
    t._latest_state = {"bookmarks": {"A": 10, "B": 20, "C": 30}}
    t._sinks_active = sinks
    t.workers = []
    t.max_parallelism = 1
    t._delete_buffer = {}
    t._records_since_checkpoint = 42
    t._drain_all = MagicMock()
    t._raise_pending_worker_error = MagicMock()
    t._reset_max_record_age = MagicMock()
    t._write_state_message = MagicMock()
    return t


def test_state_written_after_fully_successful_checkpoint():
    sinks = OrderedDict(
        A=_checkpoint_mock_sink(merge_target=object()),
        B=_checkpoint_mock_sink(merge_target=object()),
        C=_checkpoint_mock_sink(merge_target=object()),
    )
    t = _failfast_target(sinks, is_endofpipe=False)
    t.drain_all(is_endofpipe=False)
    for sink in sinks.values():
        sink.checkpoint.assert_called_once()
    t._write_state_message.assert_called_once_with({"bookmarks": {"A": 10, "B": 20, "C": 30}})
    assert t._records_since_checkpoint == 0


def test_first_sink_failure_no_state_later_sinks_not_checkpointed():
    sinks = OrderedDict(
        A=_checkpoint_mock_sink(merge_target=object()),
        B=_checkpoint_mock_sink(merge_target=object()),
        C=_checkpoint_mock_sink(merge_target=object()),
    )
    sinks["A"].checkpoint.side_effect = Exception("merge failed")
    t = _failfast_target(sinks, is_endofpipe=False)
    with pytest.raises(Exception, match="merge failed"):
        t.drain_all(is_endofpipe=False)
    sinks["B"].checkpoint.assert_not_called()
    sinks["C"].checkpoint.assert_not_called()
    t._write_state_message.assert_not_called()
    # Counter reset happens after the checkpoint loop, so a failure must skip it.
    assert t._records_since_checkpoint == 42


def test_later_sink_failure_after_earlier_success_still_fails_fast():
    sinks = OrderedDict(
        A=_checkpoint_mock_sink(merge_target=object()),
        B=_checkpoint_mock_sink(merge_target=object()),
        C=_checkpoint_mock_sink(merge_target=object()),
    )
    sinks["B"].checkpoint.side_effect = Exception("merge failed")
    t = _failfast_target(sinks, is_endofpipe=False)
    with pytest.raises(Exception, match="merge failed"):
        t.drain_all(is_endofpipe=False)
    sinks["A"].checkpoint.assert_called_once()  # A already ran (not rolled back)
    sinks["C"].checkpoint.assert_not_called()  # C never reached
    t._write_state_message.assert_not_called()
    assert t._records_since_checkpoint == 42  # not reset


def test_endofpipe_clean_up_failure_prevents_final_state():
    sinks = OrderedDict(A=MagicMock(), B=MagicMock())
    sinks["B"].clean_up.side_effect = Exception("merge failed")
    t = _failfast_target(sinks, is_endofpipe=True)
    with pytest.raises(Exception, match="merge failed"):
        t.drain_all(is_endofpipe=True)
    sinks["A"].clean_up.assert_called_once()
    t._write_state_message.assert_not_called()


def test_endofpipe_fully_successful_clean_up_writes_final_state():
    sinks = OrderedDict(A=MagicMock(), B=MagicMock())
    t = _failfast_target(sinks, is_endofpipe=True)
    t.drain_all(is_endofpipe=True)
    for sink in sinks.values():
        sink.clean_up.assert_called_once()
    t._write_state_message.assert_called_once_with({"bookmarks": {"A": 10, "B": 20, "C": 30}})


# --------------------------------------------------------------------------- #
# drain_all(is_endofpipe=False) per-sink scope gate (PQ-3547 §5.3/§10).        #
#                                                                             #
# singer-sdk's _handle_max_record_age() calls drain_all(is_endofpipe=False)   #
# directly on a wall-clock timer, bypassing the _process_state_message trigger #
# gate entirely. So the scope gate MUST also live inside drain_all's mid-run  #
# branch, per sink: eligible (batch_job/storage_write upsert) sinks MERGE via #
# checkpoint(); everything else (gcs_stage/streaming_insert/overwrite) keeps  #
# its origin/master pre_state_hook() behavior so a mid-run drain can't MERGE  #
# an empty staging table and advance the bookmark past GCS-only data.         #
# --------------------------------------------------------------------------- #
def _drain_target(sinks, method="storage_write_api"):
    """A __init__-less TargetBigQuery wired for drain_all() mid-run tests."""
    from target_bigquery.target import TargetBigQuery

    t = TargetBigQuery.__new__(TargetBigQuery)
    t._config = {"project": "p", "dataset": "d", "method": method}
    t._latest_state = {"bookmarks": {"A": 5}}
    t._sinks_active = sinks
    t.workers = []
    t.max_parallelism = 1
    t._delete_buffer = {}
    t._records_since_checkpoint = 42
    t._drain_all = MagicMock()
    t._raise_pending_worker_error = MagicMock()
    t._write_state_message = MagicMock()
    t._reset_max_record_age = MagicMock()
    return t


def test_drain_all_non_endofpipe_ineligible_method_uses_pre_state_hook():
    # method=gcs_stage: even a sink with merge_target set is OUT of scope. The
    # mid-run (max-age) drain must fall back to the origin/master pre_state_hook
    # and must NOT checkpoint (which would MERGE an empty staging table and
    # advance the bookmark past GCS-only data). STATE is still emitted.
    sink = _checkpoint_mock_sink(merge_target=object())
    t = _drain_target({"A": sink}, method="gcs_stage")
    t.drain_all(is_endofpipe=False)
    sink.pre_state_hook.assert_called_once()
    sink.checkpoint.assert_not_called()
    t._write_state_message.assert_called_once()


def test_drain_all_non_endofpipe_overwrite_sink_uses_pre_state_hook():
    # Overwrite sink under batch_job: eligible method, but overwrite_target set
    # (and merge_target None) => out of scope. pre_state_hook, not checkpoint.
    sink = _checkpoint_mock_sink(overwrite_target=object())
    t = _drain_target({"A": sink}, method="batch_job")
    t.drain_all(is_endofpipe=False)
    sink.pre_state_hook.assert_called_once()
    sink.checkpoint.assert_not_called()
    t._write_state_message.assert_called_once()


@pytest.mark.parametrize("method", ["batch_job", "storage_write_api"])
def test_drain_all_non_endofpipe_eligible_sink_checkpoints(method):
    # Eligible: batch_job/storage_write upsert sink (merge_target set, no
    # overwrite) => checkpoint() MERGEs mid-run, pre_state_hook is not used,
    # the record counter resets, and STATE is emitted.
    sink = _checkpoint_mock_sink(merge_target=object())
    t = _drain_target({"A": sink}, method=method)
    t.drain_all(is_endofpipe=False)
    sink.checkpoint.assert_called_once()
    sink.pre_state_hook.assert_not_called()
    assert t._records_since_checkpoint == 0
    t._write_state_message.assert_called_once()


def test_drain_all_non_endofpipe_eligible_checkpoint_failure_fails_fast():
    # Fail-fast still holds on the gated mid-run path: an eligible sink whose
    # checkpoint() raises propagates before STATE and before the counter reset.
    sink = _checkpoint_mock_sink(merge_target=object())
    sink.checkpoint.side_effect = Exception("merge failed")
    t = _drain_target({"A": sink}, method="batch_job")
    with pytest.raises(Exception, match="merge failed"):
        t.drain_all(is_endofpipe=False)
    t._write_state_message.assert_not_called()
    assert t._records_since_checkpoint == 42  # not reset


# --------------------------------------------------------------------------- #
# P0 (PQ-3547): a worker/load error must NEVER emit STATE.                     #
#                                                                             #
# The old drain_one() error branch recv()'d the worker error (consuming it)   #
# then called drain_all(is_endofpipe=True). Inside that nested drain,         #
# _raise_pending_worker_error found the pipe already empty and fell through   #
# to clean_up() + _write_state_message(), emitting the advanced bookmark      #
# BEFORE drain_one finally re-raised -> STATE advanced past a failed batch =   #
# data loss on resume. The fix tears down worker PROCESSES ONLY               #
# (_shutdown_workers) on the error path and never finalizes sinks or state.   #
# --------------------------------------------------------------------------- #
class _FakePipe:
    """Minimal Connection stand-in. poll() returns queued booleans (then False);
    recv() returns a fixed payload. Not a MagicMock, so poll()'s truthiness is
    exactly what we script."""

    def __init__(self, poll_results=(), recv_value=None):
        self._poll = list(poll_results)
        self._recv_value = recv_value

    def poll(self):
        if self._poll:
            return self._poll.pop(0)
        return False

    def recv(self):
        return self._recv_value


def test_worker_error_never_emits_state_through_real_drain_path():
    """The strongest P0 regression: drive the REAL TargetBigQuery.drain_all
    (is_endofpipe=False) and let the REAL singer-sdk _drain_all() call the REAL
    drain_one(), whose error poll fires. _drain_all/drain_one are NOT mocked.
    A worker error must propagate as RuntimeError with workers torn down and
    ZERO sink finalization / STATE emission -- the previously-emitted bookmark
    stays the last output."""
    from target_bigquery.target import TargetBigQuery

    t = TargetBigQuery.__new__(TargetBigQuery)
    t._config = {"project": "p", "dataset": "d", "fail_fast": True}
    t._latest_state = {"bookmarks": {"A": 7}}  # already emitted; must not advance
    t.max_parallelism = 1
    # logger is a read-only classproperty in the SDK; the real one logs fine.
    t._delete_buffer = {}

    # Fake pipes: only the error pipe fires (once).
    t.job_notification = _FakePipe([False])
    t.log_notification = _FakePipe([False])
    original_error = RuntimeError("record too large for any AppendRows request")
    t.error_notification = _FakePipe([True], recv_value=(original_error, "worker load failed"))

    # Fake queue + one alive worker so the real _shutdown_workers has work to do.
    t.queue = MagicMock()
    worker = MagicMock()
    worker.is_alive.return_value = True
    t.workers = [worker]

    # resize_worker_pool would otherwise spawn a real thread; neutralize only it.
    t.resize_worker_pool = MagicMock()

    sink = MagicMock()
    t._sinks_active = {"A": sink}

    # Record-only spies; drain_one() and _drain_all() stay REAL.
    t._flush_deletes = MagicMock()
    t._write_state_message = MagicMock()
    t._reset_max_record_age = MagicMock()

    with pytest.raises(RuntimeError):
        t.drain_all(is_endofpipe=False)

    # _shutdown_workers ran on the real path: sentinel queued, worker joined, list cleared.
    t.queue.put.assert_called_once_with(None)
    worker.join.assert_called_once()
    assert t.workers == []
    # Absolutely no sink finalization or STATE emission on the error path.
    sink.clean_up.assert_not_called()
    sink.checkpoint.assert_not_called()
    t._flush_deletes.assert_not_called()
    t._write_state_message.assert_not_called()


# --------------------------------------------------------------------------- #
# _shutdown_workers(): worker PROCESS teardown only (no sinks/state).          #
# --------------------------------------------------------------------------- #
def test_shutdown_workers_sends_sentinels_joins_once_and_clears():
    from target_bigquery.target import TargetBigQuery

    t = TargetBigQuery.__new__(TargetBigQuery)
    t.queue = MagicMock()
    workers = [MagicMock() for _ in range(3)]
    for w in workers:
        w.is_alive.return_value = True
    t.workers = list(workers)

    t._shutdown_workers()

    # One sentinel per alive worker.
    assert t.queue.put.call_count == 3
    assert all(c.args == (None,) for c in t.queue.put.call_args_list)
    # Every worker joined exactly once (no double-join, no skipped first).
    for w in workers:
        w.join.assert_called_once()
    assert t.workers == []


def test_shutdown_workers_is_noop_safe_on_empty_list():
    from target_bigquery.target import TargetBigQuery

    t = TargetBigQuery.__new__(TargetBigQuery)
    t.queue = MagicMock()
    t.workers = []
    t._shutdown_workers()  # must not raise
    t.queue.put.assert_not_called()
    assert t.workers == []


# --------------------------------------------------------------------------- #
# P2 (PQ-3547): overwrite exclusion is RUN-LEVEL on the mid-run drain.         #
# If ANY active sink is in overwrite mode, NO sink is checkpointed -- every    #
# sink uses pre_state_hook() (origin/master behavior) and STATE is emitted.    #
# --------------------------------------------------------------------------- #
def test_drain_all_non_endofpipe_mixed_upsert_and_overwrite_none_checkpoint():
    upsert = _checkpoint_mock_sink(merge_target=object())
    overwrite = _checkpoint_mock_sink(overwrite_target=object())
    t = _drain_target(OrderedDict(A=upsert, B=overwrite), method="batch_job")
    t.drain_all(is_endofpipe=False)
    # Run-level gate: coexisting overwrite target excludes the whole run.
    upsert.checkpoint.assert_not_called()
    overwrite.checkpoint.assert_not_called()
    upsert.pre_state_hook.assert_called_once()
    overwrite.pre_state_hook.assert_called_once()
    t._write_state_message.assert_called_once()  # STATE still emitted


# --------------------------------------------------------------------------- #
# Batch Job destination resolves self.table.as_ref() per enqueue, so a Job     #
# enqueued BEFORE a checkpoint carries the generation-1 table and one enqueued #
# AFTER carries generation-2 (rotated staging table). Uses the __new__-based   #
# no-BQ sink helper + a recording queue; the MERGE and staging-table creation  #
# inside checkpoint() are stubbed (no real infra), and rotation is emulated by #
# swapping self.table -- faithful to how checkpoint() rotates staging.         #
# --------------------------------------------------------------------------- #
def test_batch_job_destination_before_and_after_checkpoint_rotation(monkeypatch):
    from target_bigquery.batch_job import BigQueryBatchJobSink
    from target_bigquery.core import BigQueryTable, IngestionStrategy, Compressor, ParType

    opts = {
        "project": "p", "dataset": "d", "jsonschema": {"properties": {}},
        "transforms": {}, "ingestion_strategy": IngestionStrategy.FIXED,
    }
    gen1 = BigQueryTable(name="orders__gen1_1", **opts)
    gen2 = BigQueryTable(name="orders__gen2_1", **opts)

    s = BigQueryBatchJobSink.__new__(BigQueryBatchJobSink)
    s.client = MagicMock()
    s.merge_target = BigQueryTable(name="orders", **opts)
    s.table = gen1
    s.buffer = Compressor()
    s.global_par_typ = ParType.THREAD
    s.increment_jobs_enqueued = lambda: None

    enqueued = []
    s.global_queue = MagicMock()
    s.global_queue.put.side_effect = lambda job: enqueued.append(job)

    # REAL process_batch resolves self.table.as_ref() at enqueue time.
    s.process_batch({})  # BEFORE checkpoint -> generation 1

    # Stub the destructive halves of checkpoint(): MERGE issues a query; the
    # staging rotation swaps self.table to generation 2 (no real BQ table).
    monkeypatch.setattr(
        type(s), "_merge_staging_into_target", lambda self: self.client.query("MERGE")
    )
    monkeypatch.setattr(
        type(s), "_new_staging_table", lambda self: setattr(self, "table", gen2)
    )
    s.checkpoint()

    s.process_batch({})  # AFTER checkpoint -> generation 2

    assert len(enqueued) == 2
    assert enqueued[0].table == gen1.as_ref()
    assert enqueued[1].table == gen2.as_ref()
    assert enqueued[0].table != enqueued[1].table
