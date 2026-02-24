"""
Deployment script for Weather Pipeline using Prefect GitHub integration.

This script deploys the weather ETL flow to run from your GitHub repository.

Usage:
    # Build and deploy
    python deploy_github.py
    
    # Then start the agent
    prefect agent start -q default
"""

from prefect import flow
from prefect.runner.storage import GitRepository
from prefect_github.repository import GitHubRepository
from weather_pipeline_prefect import weather_etl_scheduled


def deploy_from_github():
    """
    Deploy the weather ETL flow using GitHub as the code source.
    
    This creates a deployment that:
    1. Pulls code from your GitHub repository
    2. Runs the scheduled weather ETL flow
    3. Stores execution history in Prefect Cloud
    """
    
    # GitHub repository configuration
    github_repo = GitHubRepository(
        repository="https://github.com/pablitotechi/api.git",
        branch="main",  # Branch to deploy from
    )
    
    print("🔧 Configuring GitHub deployment...")
    print(f"   Repository: https://github.com/pablitotechi/api.git")
    print(f"   Branch: main")
    print(f"   Flow: weather_etl_scheduled")
    print()
    
    # Build deployment
    deployment = weather_etl_scheduled.to_deployment(
        name="weather-daily-github",
        description="Weather ETL scheduled flow from GitHub",
        tags=["weather", "github", "scheduled"],
        work_queue_name="default",
        cron="0 2 * * *",  # 2:00 AM UTC every day
    )
    
    print("✅ Deployment configuration created:")
    print(f"   Name: weather-daily-github")
    print(f"   Schedule: 0 2 * * * (2:00 AM UTC daily)")
    print(f"   Queue: default")
    print()
    print("📋 Next steps:")
    print("   1. Deploy: prefect deployment apply weather-daily-github-deployment.yaml")
    print("   2. Start agent: prefect agent start -q default")
    print("   3. View runs: prefect cloud dashboard or http://127.0.0.1:4200")
    
    return deployment


if __name__ == "__main__":
    deploy_from_github()
