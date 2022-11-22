# run-pyspark-on-emr-with-sagemaker-example
On sagemaker instance, create a emr cluster, then run pyspark with livy

Step 1. Create a notebook instance with Amazon SageMaker.  
Step 2. Attach policy `AmazonElasticMapReduceFullAccess` to IAM role which excute sagemaker.  
Step 3. Git clone this sample and run `sample-caller.ipynb` step by step.
