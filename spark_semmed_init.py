import findspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F # apache 查询使用文档
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import getopt

class SemMedDB():
  # return stream path
  def getFullpath(self, streamName):
    path = r'/home/Public/SemMedDB/' + self.streams[streamName][0]
    return path

  datatypedict = {
    'bool' : BooleanType(),
    'int' : IntegerType(),
    'uint' : IntegerType(),
    'long' : LongType(),
    'ulong' : LongType(),
    'float' : FloatType(),
    'string' : StringType(),
    'DateTime' : DateType(),
  }

  # return stream schema
  def getSchema(self, streamName):
    schema = StructType()
    for field in self.streams[streamName][1]:
      fieldname, fieldtype = field.split(':')
      nullable = fieldtype.endswith('?')
      if nullable:
        fieldtype = fieldtype[:-1]
      schema.add(StructField(fieldname, self.datatypedict[fieldtype], nullable))
    return schema

  # return stream dataframe
  def getDataframe(self, streamName):
    return spark.read.format('csv').options(header='false', delimiter=',').schema(self.getSchema(streamName)).load(self.getFullpath(streamName))

  # define stream dictionary
  streams = {
    'Citations': ('semmedVER43_2023_R_CITATIONS.118705.csv', ['pmid:uint', 'issn:string?', 'dp:string?', 'edat:string?', 'pyear:uint?']),
    'Entity': ('semmedVER43_2023_R_ENTITY.118705.csv', ['entity_id:uint?', 'sentence_id:uint?', 'pmid:uint', 'cui:string?', 'name:string?', 'semtype:string?', 'gene_id:string?', 'gene_name:string?', 'text:string?', 'score:uint?','start_index:uint?', 'end_index:uint?']),
    'Predication': ('semmedVER43_2023_R_PREDICATION.118705.csv', ['predication_id:uint?', 'sentence_id:uint?', 'pmid:uint', 'predicate:string?', 'subject_cui:string?', 'subject_name:string?', 'subject_semtype:string?', 'subject_novelty:uint?', 'object_cui:string?', 'object_name:string?', 'object_semtype:string?', 'object_novelty:uint?', 'fact_value:string?', 'mod_scale:string?', 'mod_value:float?' ]),
    'PredicationAux': ('semmedVER43_2023_R_PREDICATION_AUX.118705.csv', ['predication_aux_id:uint?', 'predication_id:uint?', 'subject_text:string?', 'subject_dist:uint?', 'subject_maxdist:uint?', 'subject_start_index:uint?', 'subject_end_index:uint?', 'subject_score:uint?', 'indicator_type:string?', 'predicate_start_index:uint?', 'predicate_end_index:uint?', 'object_text:string?', 'object_dist:uint?', 'object_maxdist:uint?', 'object_start_index:uint?', 'object_end_index:uint?', 'object_score:uint?', 'curr_timestamp:DateTime?']),
    'Sentence': ('semmedVER43_2023_R_SENTENCE.118705.csv', ['sentence_id:uint?', 'pmid:uint', 'type:string?', 'number:uint?', 'sent_start_index:uint?', 'sentence:string?', 'sent_end_index:uint?', 'section_header:string?', 'normalized_section_header:string?']),
    'GenericConcept': ('semmedVER43_2023_R_GENERIC_CONCEPT.118705.csv', ['concept_id:uint?', 'cui:string?', 'preferred_name:string?'])
  }

if __name__ == '__main__':
    findspark.init('/opt/spark-3.1.2')

    opts, _ = getopt.getopt(sys.argv[1:], 'p:', ["help", "appname=", "port="])
    sparks = SparkSession \
        .builder \
        .master("spark://c8mao:7077")\
        .config("spark.executor.cores","4")\
        .config("spark.driver.memory", "20g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .appName("SemMedDB_Username_Demo")
    appnameFlag = False
    for opt, value in opts:
        if opt in ("-h", "--help"):
            print("Usage:\n\t\t-h, --help\thelp for spark_openalex_init.py\n\t\t-p=<port>, --port=<port>\tassign app spark ui to <port>.\n\t\t--appname=<name>\tset app name to <name>.")
            exit(0)
        elif opt == "--appname":
            sparks.appName(value)
            appnameFlag = True
        elif opt in ("-p", "--port"):
            sparks.config("spark.ui.port",value)

    if appnameFlag == False:
        spark.appName("SemMed_Default")
    
    spark = sparks.getOrCreate()
    display(spark)

    SMDB = SemMedDB()

    to_array = udf(lambda x: x.replace("\"","").replace("[","").replace("]","").split(','), ArrayType(StringType()))

    Citations = SMDB.getDataframe('Citations')
    Entity = SMDB.getDataframe('Entity')
    Predication = SMDB.getDataframe('Predication')
    PredicationAux = SMDB.getDataframe('PredicationAux')
    Sentence = SMDB.getDataframe('Sentence')
    GenericConcept = SMDB.getDataframe('GenericConcept')