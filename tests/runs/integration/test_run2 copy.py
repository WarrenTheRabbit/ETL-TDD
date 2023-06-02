import os
import signal
import subprocess
from etl.paths.components import Bucket
from etl.mock.infrastructure import s3_bucket
from etl.mock.infrastructure.buckets import initialise_for_project       
import colorama
from colorama import Fore, Style
colorama.init()  


def test_run2(glueContext):
    
    spark = glueContext.spark_session
    mock_bucket, process = s3_bucket.get_mock_s3_server_and_its_local_process(
                                        spark, 
                                        name=Bucket.PROJECT.value,
                                        endpoint_url="http://127.0.0.1:5000/")   
    
    initialise_for_project(mock_bucket)
    print("*"*80)
    print("The following objects" + Fore.YELLOW + " existed before testing the modules" + Fore.YELLOW + ":")
    print("*"*80)
    for obj in mock_bucket.list_all():
        s3_path = f"s3://{obj.bucket_name}/{obj.key}"
        
        if 'landing' in s3_path:
            print(Fore.RED + s3_path + Style.RESET_ALL)
        elif 'access' in s3_path:
            print(Fore.GREEN + s3_path + Style.RESET_ALL)
        elif 'raw' in s3_path:
            print(Fore.YELLOW + s3_path + Style.RESET_ALL)  
        elif 'optimised' in s3_path:
            print(Fore.BLUE + s3_path + Style.RESET_ALL)

    try:   

        import run1_provider
        run1_provider.run(spark)
        
        import run1_policyholder
        run1_policyholder.run(spark)
        
        import run1_claim
        run1_claim.run(spark)
        
        import run1_date
        run1_date.run(spark)
        
        import run2_location
        run2_location.run(spark)
        
        import run2_procedure
        run2_procedure.run(spark)
        
        
        print("*"*80)
        print("The following objects were" + Fore.YELLOW + " created during the test" + Fore.YELLOW + ":")
        print("*"*80)
        for obj in mock_bucket.list_all():
            s3_path = f"s3://{obj.bucket_name}/{obj.key}"
            
            if 'landing' in s3_path:
                print(Fore.RED + s3_path + Style.RESET_ALL)
            elif 'access' in s3_path:
                print(Fore.GREEN + s3_path + Style.RESET_ALL)
            elif 'raw' in s3_path:
                print(Fore.YELLOW + s3_path + Style.RESET_ALL)  
            elif 'optimised' in s3_path:
                print(Fore.BLUE + s3_path + Style.RESET_ALL)
                
    finally:
        mock_bucket.unload_all()
        print("*"*80)
        print("The following objects" + Fore.YELLOW + " exist after the test" + Fore.YELLOW + ":")
        print("*"*80)
        for obj in mock_bucket.list_all():
            s3_path = f"s3://{obj.bucket_name}/{obj.key}"
            
            if 'landing' in s3_path:
                print(Fore.RED + s3_path + Style.RESET_ALL)
            elif 'access' in s3_path:
                print(Fore.GREEN + s3_path + Style.RESET_ALL)
            elif 'raw' in s3_path:
                print(Fore.YELLOW + s3_path + Style.RESET_ALL)  
            elif 'optimised' in s3_path:
                print(Fore.BLUE + s3_path + Style.RESET_ALL)
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)