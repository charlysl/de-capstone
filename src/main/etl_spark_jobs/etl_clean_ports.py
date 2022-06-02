import pyspark.sql.functions as F
import pyspark.sql.types as T

from datalake.datamodel.files.i94_data_dictionary_file import I94DataDictionaryFile
from datalake.datamodel.files.ports_file import PortsFile
from datalake.datamodel.files.states_file import StatesFile

# This isn't required when running the unit test, on a spark-shell,
# but it prevents an expection when running in a cluster.
from datalake.utils import spark_helper
spark_helper.get_spark()


def load_i94_data_dictionary():
    return I94DataDictionaryFile().read()

#etl.save_clean_table(i94_data_dict_staging, 'i94_data_dictionary')

def port_dictionary(df):
    return df.select('I94PORT')

def explode_dictionary(df):
    return (
        df
        .select(F.explode('I94PORT'))
        .select(
            F.expr("trim(element_at(col, 1)) AS port_id"),
            F.expr("trim(element_at(col, 2)) AS port_desc")
        )
    )

state_suffix_expr = F.col('port_desc').rlike(', ?[A-Z]{2}$')

def filter_with_state_suffix(df):
    return df.where(state_suffix_expr)

def filter_without_state_suffix(df):
    return df.where(~state_suffix_expr)

def drop_mx_ports(df):
    """
    Mexican immigration ports are out of scope.
    """
    return df.where(~F.col('port_desc').rlike('MX$'))

def drop_unrecoverable_ports(df):
    """
    - we can't expect to extract information from them, like state and city
    - we can't expect to use them to join to other tables
    """
    return df.where(
        ~(
            F.col('port_desc').like('%No PORT Code%')
            | F.col('port_desc').like('%UNKNOWN%')
            | F.col('port_desc').like('%UNIDENTIFED%')
        )
    )

def drop_individual_ports(df):
    return df.where(
        ~(
            F.col('port_id').like('INT') 
            | F.col('port_id').like('ZZZ')
        )
    )

def clean_bps_ports(df):
        
    expr = F.expr(
    """
    IF(
        RLIKE(port_desc, ", [A-Z]{2} ((\(.*\))|(#ARPT))$"),
        REGEXP_EXTRACT(port_desc, "(.*, [A-Z]{2}) ((\(.*\))|(#ARPT))$", 1),
        port_desc
    )
    """
    )
    
    return df.withColumn('port_desc', expr)

def manually_replace(search, replace):
    return F.expr(f"replace(port_desc, '{search}', '{replace}')")

def manually_clean_ports(df):
    return (
        df
        .withColumn('port_desc', manually_replace('MARIPOSA AZ', 'MARIPOSA, AZ'))
        .withColumn('port_desc', manually_replace('WASHINGTON DC', 'WASHINGTON, DC'))
        .withColumn('port_desc', manually_replace('BELLINGHAM, WASHINGTON #INTL', 'BELLINGHAM, WA'))
    )

def split_port_desc(df):
    return (
        df
        .withColumn(
            'state_id',
            F.expr("trim(substring_index(port_desc, ',', -1))")
        )
        .withColumn(
            'name',
            F.expr("trim(regexp_extract(port_desc, '^(.*), ?[A-Z]{2}$', 1))")
        )
    )

def clean_collapsed_into_ports(df):
    return (
        df
        .withColumn('port_desc', F.expr("""
        IF(
            port_desc LIKE 'Collapsed into%',
            'INT, MN',
            port_desc
        )
        """))
    )

def drop_port_desc(df):
    return df.drop('port_desc')

def non_collapsed_ports(df):
    return (
        df
        .pipe(port_dictionary)
        .pipe(explode_dictionary)
        .pipe(drop_unrecoverable_ports)
        .pipe(drop_individual_ports)
        .pipe(clean_bps_ports)
        .pipe(manually_clean_ports) 
        .pipe(drop_mx_ports)
        .pipe(clean_collapsed_into_ports)
        .pipe(filter_with_state_suffix)
        .pipe(split_port_desc)
        .pipe(drop_port_desc)
    )

def prepare_collapsed_ports(df):
    return (
        df
        .pipe(port_dictionary)
        .pipe(explode_dictionary)
        .where(F.col('port_desc').like('Collapsed (%'))
        .select(
            F.col('port_id').alias('from_port_id'),
            F.regexp_extract(
                'port_desc',
                '^Collapsed \((.*)\)',
                1
            )
            .alias('to_port_id')
        )
    )

def merge_collapsed_ports(df):
    
    collapsed = load_i94_data_dictionary().pipe(prepare_collapsed_ports)
    
    return (
        df
        .join(
            collapsed,
            on=collapsed['to_port_id'] == df['port_id'],
            how='inner'
        )
    )

def drop_after_merge(df):
    return (
        df
        .select(
            F.expr("""
                IF(
                    ISNOTNULL(from_port_id),
                    from_port_id,
                    port_id
                )
            """)
            .alias('port_id'),
            'state_id',
            'name'
        )
    )

def collapsed_ports(df):
    return (
        df
        .pipe(non_collapsed_ports)
        .pipe(merge_collapsed_ports)
        .pipe(drop_after_merge)
    )

def ports_union(df):
    return (
        df.
        pipe(non_collapsed_ports)
        .union(
            df.pipe(collapsed_ports)
        )
    )

def remove_ports_with_non_standard_states(df):
    
    states = StatesFile().read()
    
    state_ids = states.select(F.col('state_id').alias('id'))

    return (
        df
        .join(
            state_ids,
            on=df['state_id'] == state_ids['id'],
            how='inner'
        )
        .drop('id')
    )

def save_clean_ports(df):
    PortsFile().stage(df)

def clean_ports():
    return (
        load_i94_data_dictionary()
        .pipe(ports_union)
        .pipe(remove_ports_with_non_standard_states)
        .pipe(save_clean_ports)
    )

clean_ports()









