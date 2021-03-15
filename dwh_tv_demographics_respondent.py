import csv
import re

from pyspark.sql import Row, functions as f, Window as w

from USNationalAMRLDDemo import source, dwh_name
from USNationalAMRLDDemo.Datawarehouse import dwh_household_deduplicated, dwh_person_deduplicated
from USNationalAMRLDDemo.Datawarehouse.class_demographics import DemographicsScope, pivot_loader_tmf, pivot_loader_tvo
from USNationalAMRLDDemo.Datawarehouse.class_demographics import data_loader_fused
from USNationalAMRLDDemo.Datawarehouse.codebook.demographics_respondent import demographic_respondent, \
    demographic_respondent_value
from USNationalAMRLDDemo.Datawarehouse import dwh_tv_respondent, dwh_tv_respondent_weight

raw_data_location = source['codebook']

value_descr_dict = {'True': 'Yes', 'False': 'No'}
panel = 'TV'
feed = 'dwh_tv_demographics_respondent'
skip_splitting = True

na_filter = [
    'numberofincomes',
    'ladyofhousepresentflag',
    'ladyofhouseoccupationcode',
    'homeownershipsecondaryhomestatus',
    'homestructuretype',
    'householdincomerangesdetailed',
    'longdistancecarriercode',
    'meteredmarketflag',
    'beverageusagebottledwater',
    'beverageusagecoffeeortea',
    'beverageusagesoftdrinks',
    'beverageusagetablewine',
    'collegestudentaway',
    'dbsowner',
    'headofhouseholdgender',
    'headofhouseholdhispanicspecificethnicity',
    'headofhouseholdworksoutsidehome',
    'householdincomeamount',
    'householdincomenonworking',
    'householdonlinedate',
    'nsimarketrankranges',
    'numberofoperablecomputersipadoscode',
    'numberofoperablecomputersmacoscode',
    'numberofoperablecomputersotheroscode',
    'numberofoperablecomputersothertypecode',
    'numberofoperablecomputerswindowsoscode',
    'numberofoperablecomputerscode',
    'numberofoperabledesktopscode',
    'numberofoperablelaptopscode',
    'numberofoperablemaccomputerscode',
    'numberofoperablepanlistownedworkcomputerscode',
    'numberofoperablepccomputerscode',
    'numberofoperabletabletscode',
    'numberofunknownmakecomputerscode',
    'numberofvcrs',
    'telephonestatuscode',
    'agegenderbuildingblockcode',
    'visitorstatuscode',
    'principalshopper',
    'workingwomenfulltimeflag',
    'workingwomenparttimeflag',
    'languageclasscode',
    'frequentmoviegoercode',
    'avidmoviegoercode',
    'workinghours',
    'ladyofhouseholdflag',
    'relationshiptoheadofhouseholdcode',
    'principalmoviegoerflag',
    'educationranges',
    'internetusagehome',
    'internetusagework'
]

tvu_demo = [
                'numberofincomes', 'ladyofhousepresentflag', 'ladyofhouseoccupationcode',
                'homeownershipsecondaryhomestatus', 'homestructuretype', 'householdincomerangesdetailed'
                , 'longdistancecarriercode', 'meteredmarketflag', 'beverageusagebottledwater',
                'beverageusagecoffeeortea', 'beverageusagesoftdrinks', 'beverageusagetablewine'
                , 'hdcapablereceivablehome', 'hdcapablehome', 'headofhouseholdgender',
                'headofhouseholdhispanicspecificethnicityeffectivejune282010', 'headofhouseholdworksoutsidehome'
                , 'householdincomeamount', 'householdincomenonworking', 'householdonlinedate',
                'householdwithatleast1portablevideogameplayer', 'nsimarketrankranges', 'numberofdvrs'
                , 'numberofoperablecomputersipadoscode', 'numberofoperablecomputersmacoscode',
                'numberofoperablecomputersotheroscode', 'numberofoperablecomputersothertypecode'
                , 'numberofoperablecomputerswindowsoscode', 'numberofoperablecomputerscode',
                'numberofoperabledesktopscode', 'numberofoperablelaptopscode', 'numberofoperablemaccomputerscode'
                , 'numberofoperablepanlistownedworkcomputerscode', 'numberofoperablepccomputerscode',
                'numberofoperabletabletscode', 'numberofunknownmakecomputerscode', 'internetcapabledigitaldevice'
                , 'numberofoperabletablets', 'numberofoperableiostablets', 'numberofoperableandroidostablets',
                'numberofoperableotherostablets', 'numberofoperablesmartphones'
                , 'numberofoperableiossmartphones', 'numberofoperableandroidossmartphones',
                'numberofoperableotherossmartphones', 'numberofoperableportablemediaplayers'
                , 'numberofoperableiosportablemediaplayers', 'numberofoperableotherosportablemediaplayers',
                'agegenderbuildingblockcode', 'visitorstatuscode', 'principalshopper'
                , 'workingwomenfulltimeflag', 'workingwomenparttimeflag', 'languageclasscode', 'frequentmoviegoercode',
                'avidmoviegoercode', 'workinghours', 'ladyofhouseholdflag'
                , 'relationshiptoheadofhouseholdcode', 'principalmoviegoerflag', 'internetusagehome',
                'internetusagework', 'relationshiptoheadofhouseholdcode_af_20180101'
]

def base_loader(spark_ctx):
    hive_ctx = spark_ctx.hive_ctx()

    demo_df = spark_ctx.parallelize(demographic_respondent.split('\n')).map(
        lambda x: Row(
            dat=x.split(',')
        )
    ).map(
        lambda x: Row(
            column_id=x.dat[0],
            demographic_name=x.dat[1],
            demographic_key=str(x.dat[2]),
            demographic_sequence=x.dat[3],
            is_lookup=True if x.dat[4] == '1' else False,
            use_in_target_filter=True if x.dat[5] == '1' else False
        )
    ).toDF().coalesce(1)
    
    val_df = spark_ctx.parallelize(
        csv.reader(
            demographic_respondent_value.splitlines(),
            quotechar='"',
            delimiter=',',
            quoting=csv.QUOTE_ALL,
            skipinitialspace=True
        )
    ).map(
        lambda x: Row(
            dat=x
        )
    ).map(
        lambda x: Row(
            demographic_key=str(x.dat[0]),
            demographic_value_key=x.dat[1],
            demographic_value_name=x.dat[2],
            demographic_value_sequence=x.dat[3]
        )
    ).toDF().coalesce(1)

    data = demo_df.join(
        val_df,
        on=['demographic_key'],
        how='left'
    ).select(
        'column_id',
        'demographic_key',
        'demographic_name',
        'demographic_sequence',
        f.col('is_lookup').astype('boolean'),
        f.col('use_in_target_filter').astype('boolean'),
        'demographic_value_key',
        'demographic_value_name',
        'demographic_value_sequence',
        f.when(
            f.lower(f.col('demographic_key')).isin(*tvu_demo),
            f.lit('tvu_demo')
        ).otherwise(f.lit('demo_0')).alias('parent_tree_key'),
        f.lit(feed).alias('feed'),
        f.lit('integer').alias('data_type'),
        f.when(
            f.col('demographic_key').isin(*na_filter),
            f.array(f.lit('<scope panel="' + panel + '" universe="*" subUniverses="*"/>'))
        ).when(
            f.lower(f.col('demographic_key')).like('agerange')\
            & f.col('demographic_value_name').isin('2-5', '6-8','9-11','12-14','15-17'),
            f.array(f.lit('<scope panel="' + panel + '" universe="*" subUniverses="*"/>'))
        ).when(
            f.lower(f.col('demographic_key')).like('agerangedar')\
            & f.col('demographic_value_name').isin('2-17'),
            f.array(f.lit('<scope panel="' + panel + '" universe="*" subUniverses="*"/>'))
        ).when(
            (f.lower(f.col('demographic_key')).like('age'))\
            & (f.col('demographic_value_key').astype('int') < 18),
            f.array(f.lit('<scope panel="' + panel + '" universe="*" subUniverses="*"/>'))
        ).when(
            f.lower(f.col('demographic_value_name')).like('not available'),
            f.array(f.lit('<scope panel="' + panel + '" universe="*" subUniverses="*"/>'))
        ).otherwise(
            f.array(f.lit("*"))
        ).alias('demographic_scope')
    )

    return data


def tree_loader(spark_ctx):
    hive_ctx = spark_ctx.hive_ctx()
    data = hive_ctx.createDataFrame(
        [('demo_0', '{feed}'.format(feed=feed), None, 0), ('tvu_demo', 'TV-universe demographics', 'demo_0', 1)],
        ['tree_key', 'tree_name', 'parent_tree_key', 'tree_sequence']
    ).coalesce(1)

    return data

def create_values(cols):
    values = []
    for col in cols:
        if col.is_lookup == 1:
            values.append(
                f.when(
                    f.col(col.demographic_key).isNull(),
                    f.concat_ws('_', f.lit(col.demographic_key), f.lit('9999'))
                ).when(
                    f.trim(f.col(col.demographic_key)) == '',
                    f.concat_ws('_', f.lit(col.demographic_key), f.lit('9999'))
                ).when(
                    f.length(f.regexp_extract(f.col(col.demographic_key).astype('string'), '(\d+)', 1)) > 0,
                    f.concat_ws('_', f.lit(col.demographic_key), f.col(col.demographic_key).astype('int').astype('string'))
                ).otherwise(f.concat_ws('_', f.lit(col.demographic_key), f.col(col.demographic_key)))
            )
        else:
            values.append(f.col(col.demographic_key))
    return values

def data_loader_base(spark_ctx):
    hive_ctx = spark_ctx.hive_ctx()

    demo_cols = spark_ctx.parallelize(demographic_respondent.split('\n')).map(
        lambda x: Row(
            dat=x.split(',')
        )
    ).map(
        lambda x: Row(
            demographic_key=x.dat[2],
            is_lookup=int(x.dat[4])
        )
    ).collect()

    is_lookup_keys = []
    for key in [x.demographic_key for x in demo_cols if x.is_lookup == 1]:
        is_lookup_keys.append(key)

    value_mapper = spark_ctx.parallelize(
        csv.reader(
            demographic_respondent_value.splitlines(),
            quotechar='"',
            delimiter=',',
            quoting=csv.QUOTE_ALL,
            skipinitialspace=True
        )
    ).map(
        lambda x: Row(
            dat=x
        )
    ).map(
        lambda x: Row(
            demographic_value_key='_'.join([str(x.dat[0]), str(int(x.dat[1])) if re.match('\d', str(x.dat[1])) else x.dat[1]]),
            demographic_key=str(x.dat[0]),
            demographic_value_sequence=x.dat[3]
        )
    ).map(
        lambda x: {x.demographic_value_key: [x.demographic_key, x.demographic_value_sequence]}
    ).collect()

    base_mapper = {}
    for m in value_mapper:
        demographic_value_key = list(m.keys())[0]
        demographic_key = list(m.values())[0][0]
        demographic_value_sequence = list(m.values())[0][1]
        if demographic_key in is_lookup_keys:
            base_mapper[demographic_value_key] = demographic_value_sequence

    tv_resp_df = hive_ctx.table('.'.join([dwh_name, dwh_tv_respondent.table.name])).coalesce(1)

    person_df = hive_ctx.table('.'.join([dwh_name, dwh_person_deduplicated.table.name]))

    hh_df = hive_ctx.table('.'.join([dwh_name, dwh_household_deduplicated.table.name]))

    columns_altered = ['age', 'agerange', 'agerangedar', 'agegenderbuildingblockcode', 'visitorstatuscode',
                       'workinghours', 'relationshiptoheadofhouseholdcode','internetusagehome', 'internetusagework',
                       'num_of_years_spent_in_the_usa', 'ladyofhouseoccupationcode', 'numberoftvsets',
                       'numberoftvsetswithpay', 'numberoftvsetswithwiredcable', 'numberoftvsetswithwiredcableandpay',
                       'numberofvcrs', 'headofhouseholdhispanicspecificethnicityeffectivejune282010', 'numberofcars',
                       'householdincomeamount', 'relationshiptoheadofhouseholdcode_af_20180101', 'broadcastonly', 'zerotv']

    columns_not_altered = [x.demographic_key.lower() for x in demo_cols if x.demographic_key.lower() not in columns_altered]

    demographicdata = tv_resp_df.alias('t').join(
        person_df.alias('p'),
        on=[f.col('t.household_id') == f.col('p.household_id'), f.col('t.person_id') == f.col('p.person_id')]
    ).join(
        hh_df.alias('h'),
        on=[f.col('p.household_id') == f.col('h.household_id'),
            f.col('h.start_date') <= f.col('p.end_date'),
            f.col('h.end_date') >= f.col('p.start_date')]
    ).withColumn(
        'startdate',
        f.when(
            f.col('h.start_date') > f.col('p.start_date'), f.col('h.start_date')
        ).otherwise(f.col('p.start_date'))
    ).withColumn(
        'enddate',
        f.when(
            f.col('h.end_date') < f.col('p.end_date'), f.col('h.end_date')
        ).otherwise(f.col('p.end_date'))
    ).select(
        f.col('tv_respondent_id'),
        f.col('startdate').alias('start_date'),
        f.col('enddate').alias('end_date'),
        f.when(
            f.col('p.age') == '999', f.lit(None).astype('string')
        ).otherwise(f.col('p.age')).alias('age'),
        f.when(
            f.col('p.age').astype('int').between(2, 5), f.lit(19)
        ).when(
            f.col('p.age').astype('int').between(6, 8), f.lit(20)
        ).when(
            f.col('p.age').astype('int').between(9, 11), f.lit(21)
        ).when(
            f.col('p.age').astype('int').between(12, 14), f.lit(22)
        ).when(
            f.col('p.age').astype('int').between(15, 17), f.lit(23)
        ).when(
            f.col('p.age').astype('int') == 18, f.lit(2)
        ).when(
            f.col('p.age').astype('int') == 19, f.lit(3)
        ).when(
            f.col('p.age').astype('int') == 20, f.lit(4)
        ).when(
            f.col('p.age').astype('int') == 21, f.lit(5)
        ).when(
            f.col('p.age').astype('int').between(22, 24), f.lit(6)
        ).when(
            f.col('p.age').astype('int').between(25, 29), f.lit(7)
        ).when(
            f.col('p.age').astype('int').between(30, 34), f.lit(8)
        ).when(
            f.col('p.age').astype('int').between(35, 39), f.lit(9)
        ).when(
            f.col('p.age').astype('int').between(40, 44), f.lit(10)
        ).when(
            f.col('p.age').astype('int').between(45, 49), f.lit(11)
        ).when(
            f.col('p.age').astype('int').between(50, 54), f.lit(12)
        ).when(
            f.col('p.age').astype('int').between(55, 59), f.lit(13)
        ).when(
            f.col('p.age').astype('int').between(60, 64), f.lit(14)
        ).when(
            f.col('p.age').astype('int').between(65, 69), f.lit(15)
        ).when(
            f.col('p.age').astype('int').between(70, 74), f.lit(16)
        ).when(
            f.col('p.age').astype('int').between(75, 85), f.lit(17)
        ).when(
            f.col('p.age').astype('int') > 85, f.lit(18)
        ).alias('agerange'),
        f.when(
            f.col('p.age').astype('int').between(2, 17), f.lit(1)
        ).when(
            f.col('p.age').astype('int').between(18, 20), f.lit(2)
        ).when(
            f.col('p.age').astype('int').between(21, 24), f.lit(3)
        ).when(
            f.col('p.age').astype('int').between(25, 29), f.lit(4)
        ).when(
            f.col('p.age').astype('int').between(30, 34), f.lit(5)
        ).when(
            f.col('p.age').astype('int').between(35, 39), f.lit(6)
        ).when(
            f.col('p.age').astype('int').between(40, 44), f.lit(7)
        ).when(
            f.col('p.age').astype('int').between(45, 49), f.lit(8)
        ).when(
            f.col('p.age').astype('int').between(50, 54), f.lit(9)
        ).when(
            f.col('p.age').astype('int').between(55, 64), f.lit(10)
        ).when(
            f.col('p.age').astype('int') > 64, f.lit(11)
        ).alias('agerangedar'),
        f.lit(9999).alias('agegenderbuildingblockcode'),
        f.lit(9999).alias('visitorstatuscode'),
        f.col('p.workinghours').astype('int').alias('workinghours'),
        f.when(
            f.col('startdate') < f.to_date(f.lit('2018-01-01')), f.col('p.relationshiptoheadofhouseholdcode')
        ).otherwise(f.lit(None).astype('string')).alias('relationshiptoheadofhouseholdcode'),
        f.col('p.internetusagehome').astype('int').alias('internetusagehome'),
        f.col('p.internetusagework').astype('int').alias('internetusagework'),
        f.col('p.number_of_years_spent_in_the_united_states').alias('num_of_years_spent_in_the_usa'),
        f.col('h.ladyofhouseoccupationcode').astype('int').alias('ladyofhouseoccupationcode'),
        f.col('h.numberoftvsets').astype('int').alias('numberoftvsets'),
        f.col('h.numberoftvsetswithpay').astype('int').alias('numberoftvsetswithpay'),
        f.col('h.numberoftvsetswithwiredcable').astype('int').alias('numberoftvsetswithwiredcable'),
        f.col('h.numberoftvsetswithwiredcableandpay').astype('int').alias('numberoftvsetswithwiredcableandpay'),
        f.col('h.numberofvcrs').astype('int').alias('numberofvcrs'),
        f.col('h.headofhouseholdhispanicspecificethnicity').alias('headofhouseholdhispanicspecificethnicityeffectivejune282010'),
        f.col('h.numberofcars').astype('int').alias('numberofcars'),
        f.col('h.householdincomeamount').astype('int').alias('householdincomeamount'),
        f.when(
            f.col('startdate') >= f.to_date(f.lit('2018-01-01')), f.col('p.relationshiptoheadofhouseholdcode')
        ).otherwise(f.lit(None).astype('string')).alias('RelationshipToHeadOfHouseholdCode_af_20180101'),
        f.col('h.broadcast_only').alias('broadcastonly'),
        f.col('t.zero_tv').alias('zerotv'),
        *columns_not_altered
    ).withColumn(
        'rn',
        f.row_number().over(w.partitionBy('tv_respondent_id', 'start_date').orderBy(f.col('end_date').desc()))
    ).filter(f.col('rn') == 1)

    result = demographicdata.select(
        f.col('tv_respondent_id'),
        f.col('start_date'),
        f.date_add(
            f.lead(f.col('start_date'), 1, '9999-12-31').over(w.partitionBy('tv_respondent_id').orderBy('start_date')),
            -1
        ).alias('end_date'),
        f.array(*create_values(demo_cols)).alias('values')
    )

    max_weight_date = hive_ctx.table(
        '.'.join([dwh_name, dwh_tv_respondent_weight.table.name])
    ).groupBy('tv_respondent_id').agg(
        f.max(f.col('date')).astype('date').alias('date')
    )

    return max_weight_date.alias('md').join(
        result.alias('r'),
        on=['tv_respondent_id']
    ).select(
        f.col('tv_respondent_id'),
        f.when(
            f.to_date(f.lit('2013-12-30')).between(f.col('r.start_date'), f.col('r.end_date')),
            f.to_date(f.lit('2013-12-30'))
        ).otherwise(f.col('r.start_date')).alias('start_date'),
        f.when(
            f.col('md.date').between(f.col('r.start_date'), f.col('r.end_date')),
            f.col('md.date')
        ).otherwise(f.col('r.end_date')).alias('end_date'),
        f.col('values')
    ).filter(
        f.col('end_date') >= f.to_date(f.lit('2013-12-30'))
    ).rdd.map(
        lambda x: Row(
            tv_respondent_id=int(x.tv_respondent_id),
            end_date=x.end_date,
            values=list(map(lambda v: base_mapper[v] if v in base_mapper else v, x.values)),
            start_date=x.start_date
        )
    ).toDF().repartition(100)

def pivot_fused(spark_ctx):
    return data_loader_fused(spark_ctx=spark_ctx, datawarehouse_name=dwh_name, feed=feed, respondent_id='tv_respondent_id')

def pivot_loader_tmf_(spark_ctx):
    return pivot_loader_tmf(spark_ctx=spark_ctx, skip_splitting=skip_splitting, datawarehouse_name=dwh_name, feed=feed, respondent_id='tv_respondent_id')


def pivot_loader_tvo_(spark_ctx):
    return pivot_loader_tvo(spark_ctx=spark_ctx, datawarehouse_name=dwh_name, feed=feed, respondent_id='tv_respondent_id')


dwh_tv_demographics_respondent = DemographicsScope(
    feed_name=feed,
    leaf_loader=base_loader,
    tree_loader=tree_loader,
    data_loader=pivot_loader_tmf_,
    data_loader_tvo=pivot_loader_tvo_,
    data_loader_base=data_loader_base,
    data_loader_fused=pivot_fused,
    data_type='int',
    panel='tv',
    add_scope_column=['leaf'],
    is_fuse=True,
    pivoted=True,
    safe_keys=False,
    skip_splitting=skip_splitting,
    dependencies=[dwh_household_deduplicated.table,
                  dwh_person_deduplicated.table,
                  dwh_tv_respondent.table,
                  dwh_tv_respondent_weight.table]
)

if __name__ == '__main__':
    from SparkETL.Util import PointlogicSparkContext
    from pyspark import SparkConf

    sc = PointlogicSparkContext(conf=SparkConf(), master='local[*]')
    hive_ctx = sc.hive_ctx()
    # data_loader_base(sc).show(10, False)
    # data_loader_base(sc).select('tv_respondent_id', 'start_date', 'end_date').show(100)
    base_loader(sc).filter("lower(demographic_key) like 'agerange%'").select(
        'column_id', 'demographic_key', 'demographic_value_name', 'demographic_scope'
    ).show(50, False)