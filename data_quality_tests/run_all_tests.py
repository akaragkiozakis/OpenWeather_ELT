"""
Master Data Quality Test Runner
Runs all Great Expectations tests for the OpenWeather ELT project
"""

import os
import subprocess
import sys
from datetime import datetime

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)


def log_message(message):
    """Simple logging function with Unicode support"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        print(f"[{timestamp}] {message}")
    except UnicodeEncodeError:
        # Fallback for Windows console encoding issues
        safe_message = message.encode('ascii', 'replace').decode('ascii')
        print(f"[{timestamp}] {safe_message}")


def run_test_script(script_name):
    """Run a test script and return success status"""
    log_message(f"🔄 Running {script_name}...")

    try:
        # Get the directory of this script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(current_dir, script_name)

        # Run the script
        result = subprocess.run(
            [sys.executable, script_path], capture_output=True, text=True, timeout=300
        )

        # Print the output
        if result.stdout:
            print(result.stdout)

        if result.stderr:
            print("STDERR:", result.stderr)

        success = result.returncode == 0

        if success:
            log_message(f"✅ {script_name} completed successfully!")
        else:
            log_message(f"❌ {script_name} failed with return code {result.returncode}")

        return success

    except subprocess.TimeoutExpired:
        log_message(f"⏰ {script_name} timed out after 5 minutes")
        return False
    except Exception as e:
        log_message(f"💥 Error running {script_name}: {str(e)}")
        return False


def run_all_tests():
    """Run all data quality tests"""
    log_message("🚀 Starting Master Data Quality Test Suite...")
    log_message("=" * 70)

    test_scripts = ["test_bronze_local.py", "test_silver_local.py"]

    results = {}
    total_passed = 0

    for script in test_scripts:
        print("\n" + "=" * 70)
        success = run_test_script(script)
        results[script] = success
        if success:
            total_passed += 1

    # Final summary
    print("\n" + "=" * 70)
    log_message("🎯 FINAL TEST SUMMARY")
    log_message("=" * 70)

    for script, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        log_message(f"{status} | {script}")

    log_message("=" * 70)
    total_tests = len(test_scripts)
    success_rate = (total_passed / total_tests) * 100
    log_message(
        f"📊 Overall Success Rate: {total_passed}/{total_tests} ({success_rate:.1f}%)"
    )

    if total_passed == total_tests:
        log_message("🎉 ALL DATA QUALITY TESTS PASSED!")
        log_message("✅ Your data pipeline is ready for production!")
        return True
    else:
        log_message(
            "⚠️  Some tests failed. Please review and fix issues before proceeding."
        )
        return False


def main():
    """Main function"""
    try:
        success = run_all_tests()
        return 0 if success else 1
    except KeyboardInterrupt:
        log_message("⏹️  Tests interrupted by user")
        return 130
    except Exception as e:
        log_message(f"💥 Unexpected error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
