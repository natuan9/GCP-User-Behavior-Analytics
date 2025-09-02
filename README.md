# Glamira Crawler Project

## 📁 Project Structure

```
glamira_crawler/
├── scripts/                    # Python scripts
│   ├── crawl_product_name.py   # Main crawler with threading
│   ├── process_ip_location.py  # IP geolocation processing
│   ├── test_proxy.py          # Proxy testing utilities
│   └── test_url.py            # URL testing utilities
├── config/                     # Configuration files
│   └── config.ini             # Main configuration
├── data/                      # Input data and working files
│   ├── IP-COUNTRY-REGION-CITY.BIN
│   ├── unique_ips.json
│   ├── unique_product_ids.json
│   └── processed_product_ids.json
├── logs/                      # Log files
│   ├── product_processing.log
│   └── product_processing.error.log
└── output/                    # Output files
   ├── product_names.csv     # Successful crawls
   └── failed_products.csv   # Failed crawls
```

## 🚀 Quick Start

### 1. Setup Environment
```bash
# Activate virtual environment
source ../venv/bin/activate

# Install dependencies (if needed)
pip install pymongo requests beautifulsoup4 configparser
```

### 2. Run Crawler
```bash
cd scripts/
python crawl_product_name.py
```

### 3. Check Results
```bash
# View successful crawls
head -20 output/product_names.csv

# View failed crawls
head -20 output/failed_products.csv

# Monitor logs
tail -f logs/product_processing.log
```

## ⚙️ Configuration

Edit `config/config.ini`:

- **Threading**: `max_workers = 8` (adjust based on your system)
- **Delays**: `crawl_delay_min_seconds = 0.2` (avoid being blocked)
- **MongoDB**: Update connection string if needed

## 📊 Features

- ✅ **Multi-threaded crawling** for high performance
- ✅ **Checkpoint saves** every 100 records
- ✅ **Resume capability** after interruption
- ✅ **Thread-safe data handling**
- ✅ **Detailed logging** with error tracking
- ✅ **Failed record tracking** with immediate saves

## 🔍 Monitoring

- **Real-time progress**: Check `logs/product_processing.log`
- **Error tracking**: Check `logs/product_processing.error.log`
- **Intermediate results**: `output/product_names.csv` updates every 100 records

## 🛡️ Safety Features

- **Rate limiting**: Configurable delays between requests
- **Error handling**: Graceful handling of HTTP errors
- **Data persistence**: No data loss on crashes
- **Resume support**: Continue from where you left off

## 📈 Performance

- **Speed**: 5-10x faster than single-threaded
- **Typical rate**: ~100-200 URLs per minute (depending on settings)
- **Memory efficient**: Checkpoint saves prevent memory buildup
