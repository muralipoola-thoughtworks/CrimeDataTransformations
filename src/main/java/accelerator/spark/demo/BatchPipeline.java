package accelerator.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ui.SparkUI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class BatchPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(BatchPipeline.class);
    SparkSession spark;
    Dataset<Row> cityData;
    Dataset<Row> dallasCrimeData;
    Dataset<Row> laCrimeData;
    Dataset<Row> philadelphiaCrimeData;
    private SparkConf sparkConf;
    private PipelineOptions options;

    public BatchPipeline(SparkConf sparkConf, PipelineOptions options) {
        this.sparkConf = sparkConf;
        this.options = options;
    }

    public void start() {
        LOG.info("Started spark pipeline");
        createSparkSession();

        loadData();

        processData();
    }

    public void stop() {
        LOG.info("Stopping spark pipeline...");
        this.spark.stop();
    }

    private void createSparkSession() {
        spark = SparkSession
                .builder()
                .config(this.sparkConf)
                .getOrCreate();

        SparkUI sparkUI = spark.sparkContext().ui().get();
        LOG.info("Spark UI: http://{}:{}", sparkUI.publicHostName(), sparkUI.boundPort());
    }

    private void loadData() {
        LOG.info("Loading data required for the application...");
        this.cityData = loadCityData();
        this.dallasCrimeData = loadCrimeData("Dallas");
        this.laCrimeData = loadCrimeData("LA");
        this.philadelphiaCrimeData = loadCrimeData("Philadelphia");
    }

    private Dataset<Row> loadCrimeData(String city) {
        Dataset<Row> df = spark.read().parquet(options.getDataDirectory() + "/pickup/" + city + ".parquet");
        LOG.info("========> Loaded [{}] Crime Data with [{}] rows, [{}] columns", city, df.count(), Arrays.stream(df.schema().fields()).count());
        LOG.info("========> Fields Start");
        LOG.info(Arrays.toString(df.schema().fieldNames()));
        LOG.info("========> Fields End");
        return df;
    }

    private Dataset<Row> loadCityData() {
        Dataset<Row> df = spark.read().parquet(options.getDataDirectory() + "CityData.parquet");
        LOG.info("========> Loaded City Data with [{}] rows", df.count());
        LOG.info("========> Fields Start");
        LOG.info(Arrays.toString(df.schema().fieldNames()));
        LOG.info("========> Fields End");
        return df;
    }

    private void processData() {
        this.cityData = this.cityData.withColumnRenamed("cities", "city");
        this.cityData.createOrReplaceTempView("cityData");
        this.laCrimeData.createOrReplaceTempView("laCrimeData");
        this.dallasCrimeData.createOrReplaceTempView("dallasCrimeData");
        this.philadelphiaCrimeData.createOrReplaceTempView("philadelphiaCrimeData");

        Dataset<Row> dt = spark.sql("select extract(MONTH FROM timeOccurred) as month, 'Los Angeles' as city " +
                "from laCrimeData " +
                "where lower(crimeCodeDescription)='robbery' " );
        Dataset<Row> dt1 = spark.sql("select extract(MONTH FROM startingDateTime) as month, 'Dallas' as city " +
                "from dallasCrimeData " +
                "where lower(typeOfIncident) like 'robbery%' and  lower(typeOfIncident) not like 'burglary%' " );
        Dataset<Row> dt2 = spark.sql("select extract(MONTH FROM dispatch_date_time) as month, 'Philadelphia' as city " +
                "from philadelphiaCrimeData " +
                "where lower(ucr_general_description)='robbery'" );

        Dataset<Row> totalRobberies = dt.union(dt1).union(dt2);
        totalRobberies.createOrReplaceTempView("totalRobberies");

        Dataset<Row> combinedRobberiesByMonthDF
                = spark.sql("select city, month, count(*) as robberies " +
                "from totalRobberies " +
                "group by city, month " +
                "order by city, month");
        combinedRobberiesByMonthDF.show(false);
        combinedRobberiesByMonthDF.createOrReplaceTempView("tr");

        Dataset<Row> robberyRatesByCityDf
                = spark.sql("select cityData.city, month, round(robberies/estPopulation2016 * 100,3) as robberyRate " +
                "from tr inner join cityData on tr.city=cityData.city" );
        robberyRatesByCityDf.show(false);


//        Column joinExpr = combinedRobberiesByMonthDF.col("city").equalTo(cityData.col("city"));
//        String joinType = "inner";
//        combinedRobberiesByMonthDF.join(cityData, joinExpr, joinType);
    }
}
