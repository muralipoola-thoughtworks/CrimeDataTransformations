package accelerator.spark.demo;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Console;
import scala.io.Codec;
import scala.io.Source;

import java.io.IOException;
import java.util.Properties;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            LOG.error("Usage: CrimeDataTransformations data_directory");
            System.exit(1);
        }

        PipelineOptions pipelineOptions = getPipelineOptions(args);
        SparkConf conf = getSparkAppConf();
        BatchPipeline pipeline = new BatchPipeline(conf, pipelineOptions);
        pipeline.start();
        Console.readLine();
        pipeline.stop();
    }

    private static PipelineOptions getPipelineOptions(String[] args) {
        PipelineOptions pipelineOptions = new PipelineOptions();
        pipelineOptions.setDataDirectory(args[0]);
        return pipelineOptions;
    }

    private static SparkConf getSparkAppConf() throws IOException {
        SparkConf sparkAppConf = new SparkConf();
        //Set all Spark Configs
        Properties props = new Properties();
        props.load(Source.fromFile("spark.conf", Codec.defaultCharsetCodec()).bufferedReader());

        props.forEach((k,v) -> sparkAppConf.set(k.toString(), v.toString()));
        return sparkAppConf;
    }
}
