import sys
print(f"Python path: {sys.executable}")

try:
    from googleapiclient import discovery
    print("Successfully imported googleapiclient!")
except ImportError as e:
    print(f"Import error: {e}")
    
# Print installed packages
import pkg_resources
print("\nInstalled packages:")
for package in pkg_resources.working_set:
    if 'google' in package.key:
        print(f"{package.key} - Version: {package.version}")