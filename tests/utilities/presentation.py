from etl.mock.infrastructure.s3_bucket import MockS3Bucket
import colorama
from colorama import Fore, Style


colorama.init()

def print_objects_in_bucket(message:str, mock_bucket:MockS3Bucket):
    print("*"*120)
    print(f"{Fore.MAGENTA}The objects below {message} {Style.RESET_ALL}")
    print("*"*120)

    objects =  mock_bucket.list_all()
    
    if objects == []:
        print("No objects in the bucket.")
    
    for obj in objects:
        s3_path = f"s3://{obj.bucket_name}/{obj.key}"
        
        if 'landing' in s3_path:
            print(Fore.RED + s3_path + Style.RESET_ALL)
        elif 'access' in s3_path:
            print(Fore.GREEN + s3_path + Style.RESET_ALL)
        elif 'raw' in s3_path:
            print(Fore.YELLOW + s3_path + Style.RESET_ALL)  
        elif 'optimised' in s3_path:
            print(Fore.BLUE + s3_path + Style.RESET_ALL)
    

    


def with_s3_content_reports(test_function):
    def wrapped_test(spark, mock_bucket):
        print_objects_in_bucket("existed before testing the modules:", mock_bucket)
        
        try:
            test_function(spark, mock_bucket)
            print_objects_in_bucket("were created during the test:", mock_bucket)
        finally:
            print_objects_in_bucket("exist after the test:", mock_bucket)
    
    return wrapped_test
