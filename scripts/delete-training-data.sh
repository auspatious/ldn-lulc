for t in 032/028 051/052 064/032 067/016 094/035 104/023 115/014; do aws s3 rm s3://dep-public-staging/dep_ls_lulc/test_data/0-0-2/pacific/$t/2020/samples.csv --profile $AWS_PROFILE; done
