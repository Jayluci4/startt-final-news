#!/usr/bin/env python3
"""
Enterprise News Pipeline Executor
Compatible with the new enterprise-grade architecture
"""

import os
import sys
import json
import argparse
import time
from datetime import datetime
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("📄 Loaded environment variables from .env file")
except ImportError:
    print("⚠️  python-dotenv not available - .env file support disabled")

def print_enterprise_banner():
    """Print enterprise startup banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║           🚀 ENTERPRISE NEWS INTELLIGENCE PIPELINE 🚀        ║
    ║                                                              ║
    ║  Sources: Inc42 | Entrackr | Moneycontrol | StartupNews.fyi ║
    ║           IndianStartupNews                                  ║
    ║                                                              ║
    ║  Features: ✅ Adaptive AI  ✅ Circuit Breakers             ║
    ║           ✅ Self-Healing  ✅ Advanced Analytics            ║
    ║           ✅ Multi-Algorithm Deduplication                  ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def load_enterprise_config():
    """Load enterprise configuration with enhanced .env support"""
    config = {
        'gemini_api_key': os.getenv('GEMINI_API_KEY'),
        'max_articles_per_source': int(os.getenv('MAX_ARTICLES_PER_SOURCE', '15')),
        'max_workers': int(os.getenv('MAX_WORKERS', '3')),
        'db_path': os.getenv('DB_PATH', 'news_pipeline.db'),
        'output_file': os.getenv('OUTPUT_FILE', None),
        'verbose': os.getenv('VERBOSE', 'true').lower() == 'true'
    }
    
    # Log Gemini API key status
    if config['gemini_api_key']:
        print(f"✅ GEMINI_API_KEY loaded from environment")
    else:
        print(f"⚠️  GEMINI_API_KEY not found - AI summarization will use fallback methods")
    
    # Try to load from config file if it exists
    config_files = ['pipeline_config.json', 'config.json', 'enterprise_config.json']
    for config_file in config_files:
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    file_config = json.load(f)
                    config.update(file_config)
                print(f"📄 Loaded configuration from {config_file}")
                break
            except Exception as e:
                print(f"⚠️  Warning: Could not load {config_file}: {e}")
    
    return config

def validate_enterprise_environment(config):
    """Validate enterprise environment and dependencies"""
    issues = []
    
    # Check Python version
    if sys.version_info < (3, 8):
        issues.append("❌ Python 3.8+ required for enterprise features")
    
    # Check core dependencies
    required_packages = {
        'requests': 'requests',
        'beautifulsoup4': 'bs4',
        'google-generativeai': 'google.generativeai'
    }
    
    missing_packages = []
    for package_name, import_name in required_packages.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(package_name)
    
    if missing_packages:
        issues.append(f"❌ Missing packages: {', '.join(missing_packages)}")
        issues.append(f"   Install with: pip install {' '.join(missing_packages)}")
    
    # Check optional ML dependencies
    optional_packages = {
        'scikit-learn': 'sklearn',
        'sentence-transformers': 'sentence_transformers',
        'nltk': 'nltk',
        'numpy': 'numpy',
        'python-dotenv': 'dotenv'
    }
    
    missing_optional = []
    for package_name, import_name in optional_packages.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_optional.append(package_name)
    
    if missing_optional:
        issues.append(f"⚠️  Optional packages missing: {', '.join(missing_optional)}")
        issues.append(f"   For enhanced features: pip install {' '.join(missing_optional)}")
    
    # Check Gemini API key with detailed guidance
    if not config['gemini_api_key']:
        issues.append("⚠️  GEMINI_API_KEY not found")
        issues.append("   Options to set it:")
        issues.append("   1. Create a .env file with: GEMINI_API_KEY=your_api_key_here")
        issues.append("   2. Set environment variable: export GEMINI_API_KEY=your_api_key_here")
        issues.append("   3. Pass via command line: --gemini-key your_api_key_here")
        issues.append("   AI summarization will use fallback methods without the key")
    else:
        issues.append("✅ GEMINI_API_KEY found - AI summarization enabled")
    
    return issues

def print_enterprise_summary(articles, output_file, execution_time, analytics):
    """Print comprehensive execution summary"""
    if not articles:
        print("\n❌ No articles were processed. Check the logs above for errors.")
        return
    
    print(f"\n{'='*70}")
    print("📊 ENTERPRISE PIPELINE EXECUTION SUMMARY")
    print(f"{'='*70}")
    
    # Basic metrics
    print(f"🎯 Total Articles Processed: {len(articles)}")
    print(f"⏱️  Total Execution Time: {execution_time:.2f} seconds")
    print(f"📊 Processing Rate: {len(articles)/execution_time*60:.1f} articles/minute")
    print(f"💾 Output File: {output_file}")
    
    # Source breakdown
    source_counts = {}
    quality_scores = []
    extraction_strategies = {}
    
    for article in articles:
        source = article.get('news_source', 'Unknown')
        source_counts[source] = source_counts.get(source, 0) + 1
        
        metadata = article.get('metadata', {})
        if 'quality_score' in metadata:
            quality_scores.append(metadata['quality_score'])
        
        if 'extraction_strategy' in metadata:
            strategy = metadata['extraction_strategy']
            extraction_strategies[strategy] = extraction_strategies.get(strategy, 0) + 1
    
    print(f"\n📈 Articles by Source:")
    for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(articles)) * 100
        print(f"   • {source}: {count} articles ({percentage:.1f}%)")
    
    # Quality analysis
    if quality_scores:
        avg_quality = sum(quality_scores) / len(quality_scores)
        max_quality = max(quality_scores)
        min_quality = min(quality_scores)
        
        print(f"\n🏆 Content Quality Metrics:")
        print(f"   • Average Quality Score: {avg_quality:.3f}")
        print(f"   • Highest Quality: {max_quality:.3f}")
        print(f"   • Lowest Quality: {min_quality:.3f}")
        
        # Quality distribution
        excellent = sum(1 for q in quality_scores if q >= 0.8)
        good = sum(1 for q in quality_scores if 0.6 <= q < 0.8)
        fair = sum(1 for q in quality_scores if 0.4 <= q < 0.6)
        poor = sum(1 for q in quality_scores if q < 0.4)
        
        print(f"   • Quality Distribution:")
        print(f"     - Excellent (≥0.8): {excellent} articles")
        print(f"     - Good (0.6-0.8): {good} articles")
        print(f"     - Fair (0.4-0.6): {fair} articles")
        print(f"     - Poor (<0.4): {poor} articles")
    
    # Extraction strategy breakdown
    if extraction_strategies:
        print(f"\n🔧 Extraction Strategy Usage:")
        for strategy, count in sorted(extraction_strategies.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(articles)) * 100
            print(f"   • {strategy}: {count} articles ({percentage:.1f}%)")
    
    # Sample article
    if articles:
        sample = articles[0]
        print(f"\n📄 Sample Article:")
        print(f"   Title: {sample.get('news_title', 'N/A')[:80]}...")
        print(f"   Source: {sample.get('news_source', 'N/A')}")
        print(f"   Quality: {sample.get('metadata', {}).get('quality_score', 'N/A')}")
        print(f"   Strategy: {sample.get('metadata', {}).get('extraction_strategy', 'N/A')}")
        if sample.get('ai_summary'):
            print(f"   AI Summary: {sample.get('ai_summary', '')[:120]}...")
    
    # Performance insights
    content_lengths = [len(a.get('news_content', '')) for a in articles]
    if content_lengths:
        avg_length = sum(content_lengths) / len(content_lengths)
        print(f"\n📝 Content Analysis:")
        print(f"   • Average Content Length: {avg_length:,.0f} characters")
        print(f"   • Total Content Extracted: {sum(content_lengths):,.0f} characters")
    
    print(f"\n✅ Enterprise pipeline completed successfully!")
    print(f"📁 Open {output_file} to view all processed articles with metadata")
    print(f"{'='*70}")

def execute_pipeline(config, args=None):
    """
    Core logic to initialize and run the enterprise pipeline.
    This function is designed to be callable from other scripts.
    """
    if args is None:
        # Create a default args namespace if not provided
        from argparse import Namespace
        args = Namespace(quiet=False, analytics=False, reset_db=False)
    try:
        # Record start time
        start_time = datetime.now()
        # Import and create enterprise pipeline
        if not args.quiet:
            print(f"\n🔄 Initializing enterprise pipeline...")
        try:
            from master_pipeline import create_enterprise_pipeline
        except ImportError as e:
            print(f"❌ Failed to import enterprise pipeline: {e}")
            print(f"💡 Make sure master_pipeline.py contains the enterprise version")
            return {'success': False, 'error': str(e)}
        # Reset database if requested
        if args.reset_db and os.path.exists(config['db_path']):
            os.remove(config['db_path'])
            if not args.quiet:
                print(f"🗑️  Reset database: {config['db_path']}")
        # Create pipeline instance
        pipeline = create_enterprise_pipeline(
            gemini_api_key=config['gemini_api_key'],
            db_path=config['db_path']
        )
        if not args.quiet:
            print(f"🚀 Starting enterprise pipeline execution...")
        # Execute pipeline
        articles = pipeline.process_all_sources(
            max_articles_per_source=config['max_articles_per_source'],
            max_workers=config['max_workers'],
            sources=config.get('sources') # Pass sources to pipeline
        )
        # Save results
        output_file = pipeline.save_to_json(articles, config.get('output_file'))
        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds()
        # Get analytics
        analytics = None
        if args.analytics or not args.quiet:
            try:
                analytics = pipeline.get_analytics_dashboard()
            except Exception as e:
                if not args.quiet:
                    print(f"⚠️  Analytics generation failed: {e}")
        # Print summary (unless quiet)
        if not args.quiet:
            print_enterprise_summary(articles, output_file, execution_time, analytics)
        else:
            print(f"✅ Processed {len(articles)} articles in {execution_time:.2f}s → {output_file}")
        # Show detailed analytics if requested
        if args.analytics and analytics:
            print(f"\n📊 DETAILED ANALYTICS:")
            print(f"{'='*50}")
            print(json.dumps(analytics, indent=2, default=str))
        # Return data for programmatic use
        return {
            'success': True,
            'articles_count': len(articles),
            'output_file': output_file,
            'execution_time': execution_time,
            'analytics': analytics,
            'articles': articles
        }
    except KeyboardInterrupt:
        print(f"\n⛔ Pipeline interrupted by user")
        return {'success': False, 'error': 'Interrupted by user'}
    except Exception as e:
        print(f"\n❌ Pipeline execution failed: {e}")
        if not args.quiet:
            import traceback
            traceback.print_exc()
        return {'success': False, 'error': str(e)}

def main():
    """Main enterprise execution function"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Enterprise News Intelligence Pipeline')
    parser.add_argument('--articles', type=int, help='Max articles per source (default: 15)')
    parser.add_argument('--workers', type=int, help='Max concurrent workers (default: 3)')
    parser.add_argument('--output', type=str, help='Output filename')
    parser.add_argument('--db-path', type=str, help='Database path (default: news_pipeline.db)')
    parser.add_argument('--quiet', action='store_true', help='Quiet mode (minimal output)')
    parser.add_argument('--no-ai', action='store_true', help='Skip AI summarization')
    parser.add_argument('--analytics', action='store_true', help='Show detailed analytics')
    parser.add_argument('--reset-db', action='store_true', help='Reset database before running')
    parser.add_argument('--sources', nargs='+', help='List of sources to process')
    parser.add_argument('--gemini-key', type=str, help='Gemini API key (overrides .env and environment variable)')
    
    args = parser.parse_args()
    
    # Print banner unless in quiet mode
    if not args.quiet:
        print_enterprise_banner()
    
    # Load configuration
    config = load_enterprise_config()
    
    # Override config with command line arguments
    if args.articles:
        config['max_articles_per_source'] = args.articles
    if args.workers:
        config['max_workers'] = args.workers
    if args.output:
        config['output_file'] = args.output
    if args.db_path:
        config['db_path'] = args.db_path
    if args.no_ai:
        config['gemini_api_key'] = None
    if args.sources:
        config['sources'] = args.sources
    if args.gemini_key:
        config['gemini_api_key'] = args.gemini_key
        print(f"🔑 Using Gemini API key from command line argument")
    
    # Validate environment
    if not args.quiet:
        validation_issues = validate_enterprise_environment(config)
        if validation_issues:
            print("🔍 Environment Check:")
            for issue in validation_issues:
                print(f"   {issue}")
            
            # Check if we can continue
            critical_issues = [i for i in validation_issues if i.startswith('❌')]
            if critical_issues:
                response = input(f"\n❓ Critical issues found. Continue anyway? (y/N): ")
                if response.lower() not in ['y', 'yes']:
                    print("❌ Exiting. Please resolve critical issues first.")
                    sys.exit(1)
    
    # Print configuration (unless quiet)
    if not args.quiet:
        print(f"\n⚙️  Enterprise Configuration:")
        print(f"   📰 Max articles per source: {config['max_articles_per_source']}")
        print(f"   🧵 Concurrent workers: {config['max_workers']}")
        print(f"   🤖 AI summarization: {'✅ Enabled' if config['gemini_api_key'] else '❌ Disabled'}")
        print(f"   💾 Database: {config['db_path']}")
        if config['output_file']:
            print(f"   📁 Output file: {config['output_file']}")
    
    result = execute_pipeline(config, args)
    if not result.get('success'):
        sys.exit(1)
    return result

if __name__ == "__main__":
    result = main()