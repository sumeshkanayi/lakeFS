id=$(aws --endpoint-url http://s3.local.lakefs.io:8000 s3api create-multipart-upload --bucket demo1 --key main/india| jq ."UploadId" -r)
echo "upload id is $id"
echo $(aws --endpoint-url http://s3.local.lakefs.io:8000 s3api upload-part --bucket demo1 --key main/india --part-number 0 --body temp_10MB_file_1.txt --upload-id $id |jq ".ETag" -r)
echo $(aws --endpoint-url http://s3.local.lakefs.io:8000 s3api upload-part --bucket demo1 --key main/india --part-number 1 --body temp_10MB_file_2.txt --upload-id $id |jq ".ETag" -r)
#aws --endpoint-url http://s3.local.lakefs.io:8000 s3api complete-multipart-upload --multipart-upload file://etags.json --bucket demo1 --key main/india  --upload-id $id
