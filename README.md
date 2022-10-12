# empty-s3-bucket

Little app to clear out an S3 bucket of its contents.

It will generate a list of the objects in the bucket and then try to remove them in batches.
Batches are limited to a maximum of 1000.
This is due to the request limit in AWS.

Versioned objects are included, deleted, latest and old...

**EVERYTHING IS DELETED**

At the end of running this tool you would be able to delete a bucket as nothing would be left in it.

> Use with cation as once these files are deleted they really are gone forever!
