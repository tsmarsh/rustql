#!/usr/bin/env python3
"""
RustQL Coverage Analysis Tool
Analyzes tarpaulin coverage data and generates comprehensive reports
"""

import xml.etree.ElementTree as ET
import os
import json
from collections import defaultdict
import sys

def parse_cobertura_xml(xml_file):
    """Parse Cobertura XML coverage report"""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Extract overall coverage statistics
        coverage_data = {
            'total_lines': 0,
            'covered_lines': 0,
            'line_rate': 0.0,
            'total_branches': 0,
            'covered_branches': 0,
            'branch_rate': 0.0,
            'modules': [],
            'files': []
        }
        
        # Parse overall coverage stats
        coverage_data['total_lines'] = int(root.attrib.get('lines-valid', '0'))
        coverage_data['covered_lines'] = int(root.attrib.get('lines-covered', '0'))
        coverage_data['line_rate'] = float(root.attrib.get('line-rate', '0.0'))
        
        coverage_data['total_branches'] = int(root.attrib.get('branches-valid', '0'))
        coverage_data['covered_branches'] = int(root.attrib.get('branches-covered', '0'))
        coverage_data['branch_rate'] = float(root.attrib.get('branch-rate', '0.0'))
        
        # Parse module and file coverage
        for package in root.findall('packages/package'):
            module_name = package.attrib.get('name', 'unknown')
            
            module_stats = {
                'name': module_name,
                'line_rate': float(package.attrib.get('line-rate', '0.0')),
                'branch_rate': float(package.attrib.get('branch-rate', '0.0')),
                'complexity': float(package.attrib.get('complexity', '0.0')),
                'files': []
            }
            
            for class_elem in package.findall('classes/class'):
                filename = class_elem.attrib.get('filename', 'unknown')
                
                file_stats = {
                    'name': filename,
                    'line_rate': float(class_elem.attrib.get('line-rate', '0.0')),
                    'branch_rate': float(class_elem.attrib.get('branch-rate', '0.0')),
                    'complexity': float(class_elem.attrib.get('complexity', '0.0')),
                    'lines': int(class_elem.attrib.get('lines-valid', '0')),
                    'covered_lines': int(class_elem.attrib.get('lines-covered', '0')),
                    'branches': int(class_elem.attrib.get('branches-valid', '0')),
                    'covered_branches': int(class_elem.attrib.get('branches-covered', '0'))
                }
                
                module_stats['files'].append(file_stats)
            
            coverage_data['modules'].append(module_stats)
        
        return coverage_data
        
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return None

def generate_text_report(coverage_data):
    """Generate a comprehensive text report"""
    if not coverage_data:
        return "No coverage data available"
    
    report = []
    report.append("=" * 80)
    report.append("RUSTQL COVERAGE ANALYSIS REPORT")
    report.append("=" * 80)
    report.append("")
    
    # Overall statistics
    report.append("📊 OVERALL COVERAGE STATISTICS")
    report.append("-" * 40)
    report.append(f"Total Lines:          {coverage_data['total_lines']:,}")
    report.append(f"Covered Lines:        {coverage_data['covered_lines']:,}")
    report.append(f"Line Coverage:        {coverage_data['line_rate']*100:.1f}%")
    report.append(f"Total Branches:       {coverage_data['total_branches']:,}")
    report.append(f"Covered Branches:     {coverage_data['covered_branches']:,}")
    report.append(f"Branch Coverage:      {coverage_data['branch_rate']*100:.1f}%")
    report.append("")
    
    # Module breakdown
    report.append("📁 MODULE COVERAGE BREAKDOWN")
    report.append("-" * 40)
    
    modules_sorted = sorted(coverage_data['modules'], key=lambda x: x['line_rate'], reverse=True)
    
    for module in modules_sorted:
        report.append(f"Module: {module['name']}")
        report.append(f"  Line Coverage:   {module['line_rate']*100:.1f}%")
        report.append(f"  Branch Coverage: {module['branch_rate']*100:.1f}%")
        report.append(f"  Complexity:      {module['complexity']:.1f}")
        report.append("")
    
    # Detailed file analysis
    report.append("📄 DETAILED FILE COVERAGE")
    report.append("-" * 40)
    
    all_files = []
    for module in coverage_data['modules']:
        for file_info in module['files']:
            file_info['module'] = module['name']
            all_files.append(file_info)
    
    # Sort files by coverage (lowest first to highlight problem areas)
    all_files_sorted = sorted(all_files, key=lambda x: x['line_rate'])
    
    for file_info in all_files_sorted:
        coverage_pct = file_info['line_rate'] * 100
        status = "✅ EXCELLENT" if coverage_pct >= 90 else "⚠️  GOOD" if coverage_pct >= 70 else "❌ NEEDS WORK"
        
        report.append(f"File: {file_info['name']}")
        report.append(f"  Module:        {file_info['module']}")
        report.append(f"  Line Coverage: {coverage_pct:.1f}% {status}")
        report.append(f"  Lines:         {file_info['covered_lines']}/{file_info['lines']}")
        report.append(f"  Branch Coverage: {file_info['branch_rate']*100:.1f}%")
        report.append(f"  Complexity:    {file_info['complexity']:.1f}")
        report.append("")
    
    # Summary and recommendations
    report.append("🎯 SUMMARY & RECOMMENDATIONS")
    report.append("-" * 40)
    
    overall_coverage = coverage_data['line_rate'] * 100
    if overall_coverage >= 80:
        report.append("✅ EXCELLENT: Overall coverage is very good!")
    elif overall_coverage >= 60:
        report.append("⚠️  GOOD: Overall coverage is decent but could be improved")
    else:
        report.append("❌ NEEDS WORK: Overall coverage needs significant improvement")
    
    report.append(f"Current overall line coverage: {overall_coverage:.1f}%")
    
    # Find files with lowest coverage
    low_coverage_files = [f for f in all_files if f['line_rate'] * 100 < 50]
    if low_coverage_files:
        report.append("")
        report.append("🔍 Files needing attention (< 50% coverage):")
        for file_info in low_coverage_files:
            report.append(f"  • {file_info['name']}: {file_info['line_rate']*100:.1f}%")
    
    return "\n".join(report)

def generate_json_report(coverage_data, output_file):
    """Generate JSON report for programmatic analysis"""
    with open(output_file, 'w') as f:
        json.dump(coverage_data, f, indent=2)

def main():
    xml_file = 'coverage/cobertura.xml'
    
    if not os.path.exists(xml_file):
        print(f"Error: Coverage file {xml_file} not found")
        print("Please run: cargo tarpaulin --out Xml --output-dir coverage")
        sys.exit(1)
    
    print("🔍 Analyzing coverage data...")
    coverage_data = parse_cobertura_xml(xml_file)
    
    if not coverage_data:
        print("Failed to parse coverage data")
        sys.exit(1)
    
    # Generate reports
    print("📊 Generating text report...")
    text_report = generate_text_report(coverage_data)
    
    print("📊 Generating JSON report...")
    generate_json_report(coverage_data, 'coverage_report.json')
    
    # Save text report
    with open('coverage_report.txt', 'w') as f:
        f.write(text_report)
    
    print("✅ Coverage analysis complete!")
    print("📄 Reports generated:")
    print("  • coverage_report.txt (human-readable)")
    print("  • coverage_report.json (machine-readable)")
    print("  • coverage/html/tarpaulin-report.html (interactive HTML)")
    print("")
    print(text_report)

if __name__ == "__main__":
    main()