Steps to execute the application to get the results of the given 10 analysis:

1. install the requirements mentioned in requirements.txt file:
   =>   pip3 install -r requirements.txt

2. There are two approaches, we can run the application:
    
    a. All the 10 analysis at once and save the results into the given output path. To execute this following
        spark-submit needs to be executed.
        Execute the below command in the main folder (i.e. bcg-case-study)
        
    spark-submit \
    --py-files \
    src/utils/spark_client.py,\
    src/utils/project_utils.py,\
    src/config/analysis_config.py,\
    src/batch/common_batch_job.py,\
    src/batch/transform_methods.py \
    src/driver/driver_job.py \
    analysis_number=<analysis_number>  \
    input_path=<path> \
    output_path=<path>

    Need to pass command line argument, the analysis_number as all and input_path and output_path can be specified if you want a custom location.
    By default, it will use the path of input_data path and output path present in the project directory.
    
    Like this:

   spark-submit \
   --py-files \
   src/utils/spark_client.py,\
   src/utils/project_utils.py,\
   src/config/analysis_config.py,\
   src/batch/common_batch_job.py,\
   src/batch/transform_methods.py \
   src/driver/driver_job.py \
   analysis_number=all \
   input_path=/Users/jikusandilya/Desktop/Python/bcg-case-study/input_data \
   output_path=/Users/jikusandilya/Desktop/Python/bcg-case-study/output

    
   b. One analysis at one time and save the result.
      For that, replace analysis_number as analysis_1 or analysis_2 or .... or analysis_10.
        
   Like this:
    
    spark-submit \
    --py-files \
    src/utils/spark_client.py,\
    src/utils/project_utils.py,\
    src/config/analysis_config.py,\
    src/batch/common_batch_job.py,\
    src/batch/transform_methods.py \
    src/driver/driver_job.py \
    analysis_number=analysis_1 \
    input_path=/Users/jikusandilya/Desktop/Python/bcg-case-study/input_data \
    output_path=/Users/jikusandilya/Desktop/Python/bcg-case-study/output


    spark-submit \
    --py-files \
    src/utils/spark_client.py,\
    src/utils/project_utils.py,\
    src/config/analysis_config.py,\
    src/batch/common_batch_job.py,\
    src/batch/transform_methods.py \
    src/driver/driver_job.py \
    analysis_number=analysis_10 \
    input_path=/Users/jikusandilya/Desktop/Python/bcg-case-study/input_data \
    output_path=/Users/jikusandilya/Desktop/Python/bcg-case-study/output

    Again, input_path and output_path are optional. By default, it will use the path of input_data path and output path present in the project directory.
    
    If you want a custom location for input data and output result, then it needs to pass in the comman line argument like above.


3. There are two kinds of results.
    1. Count of records => Count of records are saved as text file
    2. dataframe => saved as csv file in the given output location.