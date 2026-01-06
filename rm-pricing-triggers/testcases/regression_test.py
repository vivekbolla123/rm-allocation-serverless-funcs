"""
Regression Test Runner

This script runs tests and compares outputs against baseline files.
Use this to ensure changes don't break existing functionality.
"""

import os
import sys
import pandas as pd
import subprocess
from pathlib import Path

# Configuration
MODULES_TO_TEST = ['sales_fare', 'ladder_gen', 'action_sect']
BASELINE_DIR = os.path.join(os.path.dirname(__file__), 'baselines')
OUTPUTS_DIR = os.path.join(os.path.dirname(__file__), 'outputs', 'generated_outputs')

def ensure_baseline_dir():
    """Create baseline directory if it doesn't exist."""
    os.makedirs(BASELINE_DIR, exist_ok=True)

def run_module_test(module_name):
    """Run test for a specific module."""
    print(f"\n🧪 Testing module: {module_name}")
    
    # Update the TEST_MODULE in simple_mock_helper.py
    helper_file = os.path.join(os.path.dirname(__file__), 'simple_mock_helper.py')
    
    # Read current content
    with open(helper_file, 'r') as f:
        content = f.read()
    
    # Replace TEST_MODULE line
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if line.startswith('TEST_MODULE = '):
            lines[i] = f"TEST_MODULE = '{module_name}'"
            break
    
    # Write back
    with open(helper_file, 'w') as f:
        f.write('\n'.join(lines))
    
    # Run the test
    try:
        result = subprocess.run([
            sys.executable, 'simple_test_runner.py'
        ], cwd=os.path.dirname(__file__), capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ {module_name} test completed successfully")
            return True
        else:
            print(f"❌ {module_name} test failed:")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"❌ Error running {module_name} test: {str(e)}")
        return False

def compare_outputs(module_name):
    """Compare generated output with baseline."""
    output_files = {
        'sales_fare': 'sales_fare_output.csv',
        'ladder_gen': 'ladder_gen_output.csv',
        'action_sect': 'actionable_sectors_output.csv'
    }
    
    output_file = output_files.get(module_name)
    if not output_file:
        print(f"⚠️  No output file mapping for {module_name}")
        return True
    
    generated_path = os.path.join(OUTPUTS_DIR, output_file)
    baseline_path = os.path.join(BASELINE_DIR, output_file)
    
    if not os.path.exists(generated_path):
        print(f"⚠️  Generated output not found: {generated_path}")
        return False
    
    if not os.path.exists(baseline_path):
        print(f"📋 No baseline found for {module_name}, creating baseline...")
        # Copy generated file as new baseline
        import shutil
        shutil.copy2(generated_path, baseline_path)
        print(f"✅ Baseline created: {baseline_path}")
        return True
    
    # Compare files
    try:
        generated_df = pd.read_csv(generated_path)
        baseline_df = pd.read_csv(baseline_path)
        
        # Check if DataFrames are equal
        if generated_df.equals(baseline_df):
            print(f"✅ {module_name} output matches baseline")
            return True
        else:
            print(f"❌ {module_name} output differs from baseline:")
            
            # Show differences
            if len(generated_df) != len(baseline_df):
                print(f"   Row count: Generated={len(generated_df)}, Baseline={len(baseline_df)}")
            
            if list(generated_df.columns) != list(baseline_df.columns):
                print(f"   Column differences:")
                print(f"     Generated: {list(generated_df.columns)}")
                print(f"     Baseline:  {list(baseline_df.columns)}")
            
            # Show first few differing rows if same structure
            if generated_df.shape == baseline_df.shape:
                diff_mask = (generated_df != baseline_df).any(axis=1)
                if diff_mask.any():
                    print(f"   First 3 differing rows:")
                    differing_rows = generated_df[diff_mask].head(3)
                    print(differing_rows.to_string())
            
            return False
            
    except Exception as e:
        print(f"❌ Error comparing {module_name} output: {str(e)}")
        return False

def update_baseline(module_name):
    """Update baseline with current output."""
    output_files = {
        'sales_fare': 'sales_fare_output.csv',
        'ladder_gen': 'ladder_gen_output.csv', 
        'action_sect': 'actionable_sectors_output.csv'
    }
    
    output_file = output_files.get(module_name)
    if not output_file:
        print(f"⚠️  No output file mapping for {module_name}")
        return False
    
    generated_path = os.path.join(OUTPUTS_DIR, output_file)
    baseline_path = os.path.join(BASELINE_DIR, output_file)
    
    if not os.path.exists(generated_path):
        print(f"❌ Generated output not found: {generated_path}")
        return False
    
    try:
        import shutil
        shutil.copy2(generated_path, baseline_path)
        print(f"✅ Updated baseline for {module_name}")
        return True
    except Exception as e:
        print(f"❌ Error updating baseline for {module_name}: {str(e)}")
        return False

def main():
    """Main regression test function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run regression tests for pricing modules')
    parser.add_argument('--update-baselines', action='store_true', 
                       help='Update baselines with current outputs')
    parser.add_argument('--module', choices=MODULES_TO_TEST, 
                       help='Test specific module only')
    
    args = parser.parse_args()
    
    ensure_baseline_dir()
    
    modules_to_test = [args.module] if args.module else MODULES_TO_TEST
    
    print("🚀 Starting Regression Tests")
    print(f"📁 Baseline Directory: {BASELINE_DIR}")
    print(f"📁 Output Directory: {OUTPUTS_DIR}")
    print("-" * 50)
    
    all_passed = True
    
    for module in modules_to_test:
        # Run the test
        if not run_module_test(module):
            all_passed = False
            continue
        
        if args.update_baselines:
            # Update baseline
            update_baseline(module)
        else:
            # Compare with baseline
            if not compare_outputs(module):
                all_passed = False
    
    print("-" * 50)
    if all_passed:
        print("✅ All regression tests passed!")
    else:
        print("❌ Some regression tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()