import os
import yaml
import json
import logging
from time import sleep
from datetime import datetime
# PyGitHub uses iterators to handle pagination seamlessly. 
from github import Github, RateLimitExceededException


# Load the GitHub token from an environment variable
github_token = os.environ.get('GITHUB_TOKEN')
github = Github(github_token)

def find_project_root(current_directory):
    """
    Find the root directory of the project by iteratively checking for the existence of the specified project folder name in the current directory and its parent directories.

    Args:
        current_directory (str): The starting directory to begin the search.

    Returns:
        str: The path to the project's root directory, or None if the project folder name is not found.
    """
    project_folder_name = 'compliance_data_pipeline'
    while True:
        # Check if the specified project folder name exists in the current directory
        if project_folder_name in os.listdir(current_directory):
            return os.path.join(current_directory, project_folder_name)  # Found the project root folder
        # Move up one directory level
        current_directory = os.path.dirname(current_directory)
        # Stop if we've reached the root directory (i.e., cannot move up further)
        if current_directory == os.path.dirname(current_directory):
            break
    return None  # Unable to determine the project root


# Get the path to the project's root folder
current_dir = os.path.abspath(os.getcwd())
project_root = find_project_root(current_dir)

# Specify the path to the log file (github_api.log) in the log folder
log_file_path = os.path.join(project_root, 'logs', 'github_pr_extract.log')

# Configure logging to write to the log file in the project log folder
logging.basicConfig(filename=log_file_path, level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

config_file_path = os.path.join(project_root, 'github_profiles.yml')
with open(config_file_path, 'r') as file:
    profiles = yaml.safe_load(file)

today_date = datetime.now().strftime('%Y-%m-%d')

for org_key, profiles in profiles.items():
    
    for profile_name in profiles:
        
        while True:
            try:
                
                profile = github.get_organization(profile_name)
                
                profile_dir = os.path.join(project_root, 'data_lake', 'github', 'pull_requests', today_date, org_key, profile_name)
                os.makedirs(profile_dir, exist_ok=True)
                
                for repo in profile.get_repos():

                    repo_data = {
                        "full_name": repo.full_name, # Organization Name
                        "name": repo.name, # repository_name
                        "id": repo.id, # repository_id
                        "owner": repo.owner.login # repository_owner
                    }
                    
                    pull_requests = []
                    for pr in repo.get_pulls():
                        pull_requests.append({
                            "title": pr.title,
                            "state": pr.state,
                            "created_at": pr.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                            "updated_at": pr.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                            "closed_at": pr.closed_at.strftime("%Y-%m-%d %H:%M:%S") if pr.closed_at else None,
                            "merged_at": pr.merged_at.strftime("%Y-%m-%d %H:%M:%S") if pr.merged_at else None,
                            "base_branch": pr.base.ref,
                            "head_branch": pr.head.ref
                        })
                        
                    repo_data['pull_requests'] = pull_requests


                    # # Save repo data to a JSON file
                    with open(f'{profile_dir}/{repo.name}.json', 'w') as json_file:
                        json.dump(repo_data, json_file, indent=4)
                
                break
                        
            except RateLimitExceededException:
                # Handle rate limits by pausing and retrying if necessary
                while github.get_rate_limit().core.remaining == 0:
                    reset_time = github.get_rate_limit().core.reset
                    sleep((reset_time - datetime.now()).total_seconds() + 10)  # Wait additional 10 seconds
                    
            except Exception as e:
                # Log the exception details to the error.log file
                logging.error(f"An unexpected error occurred: {str(e)}")
                break