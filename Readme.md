run datagen.py to generate input files
run replicate.py to replicate the results

please replace the path in replicate_1.py and replicate_2.py to your own path

before running replicate_2.py, please run replicate_1.py first and in line 30 replace the path with file that was created as effect of clustering by replicate_1.py

one can verify that schema of newly created file changed during clustering by running the following command:
`parquet-tools inspect testtable/year=2022/e09e6824-65b2-46df-ad78-e77561bb69ea-0_0-156-179_20230323184211330.parquet`