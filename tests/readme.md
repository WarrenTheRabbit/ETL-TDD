# Why are my tests failing?

## Path tests
Your path tests will fail if your `Bucket` 
enum does not point to an S3 bucket containing the files and folders your paths are testing. 

### Possible Errors
`timestamps/get_timestamp_of_most_recently_created_file` will throw a `ValueError` if you are looking for the most recently created (non-existent) file at a (non-existent) address.

```python
        if not file_paths:
>           raise ValueError("No files found in bucket.")
E           ValueError: No files found in bucket.
```


`timestamps/get_lexicographically_highest_subdirectory` will fail throw an `IndexError` if
you are trying to select the most recent timestamp of (non-existent) folders at a (non-existent) address.

```python
        candidate_dates = []
        for obj in bucket.objects.filter(Prefix=prefix):
            subdirs = obj.key.split('/')
            leaf = subdirs[-2:][0]
            if leaf.isdigit():
                candidate_dates.append(leaf)
        try:
>           most_recent = sorted(candidate_dates)[-1]
E           IndexError: list index out of range
```

### What can I do?
I recommend pointing all path 
tests to a test bucket with the necessary files; for example, using `run_everything.py` to load a `Bucket.TEST` location.

Otherwise, only run your path tests when your current target is fully loaded.

## Integration Tests

### Possible Errors

You choose the endpoint to work with (local or AWS) the first time you invoke 
one a `get..` static method. 

If you first use the `getMockInstance` creation method, your code will work with 
a local endpoint for the entire session.

```python
>>> s3 = S3Resource.getMockInstance().meta.client._endpoint
>>> s3
s3(http://127.0.0.1:5000/)
>>> s3 = S3Resource.getInstance().meta.client._endpoint
>>> s3
s3(http://127.0.0.1:5000/)
```

But if you first use the `getInstance` creation method, your code will work with 
a AWS endpoint.

```python
>>> s3 = S3Resource.getInstance().meta.client._endpoint
>>> s3
https://s3.ap-southeast-2.amazonaws.com
>>> s3 = S3Resource.getMockInstance().meta.client._endpoint
>>> s3
https://s3.ap-southeast-2.amazonaws.com
```
