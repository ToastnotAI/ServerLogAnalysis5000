import logging
import re
import pandas as pd
import os
from DataProcessing.DataLoader import DataContainer
from DataExploration.DataVisualiser import PieChartVisualiser, BarChartVisualiser, line_chart_visualiser

logger = logging.getLogger(__name__)
#get absolute path of current directory
path = os.path.abspath(os.path.dirname(__file__))

if not os.path.exists(os.path.join(path, 'SelfLogs')):
    os.makedirs(os.path.join(path, 'SelfLogs'))

logging.basicConfig(filename = os.path.join(path, 'SelfLogs/AppLog.log'), level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def loadData(check_existing = True):
    """Loads the data using DataContainer and processes it if necessary."""
    if check_existing:
        data_container = DataContainer()
    else:
        data_container = DataContainer(file_location='../AccessLogs/apache_access.log.log')
        data_container.process_data(overwrite=True)
    return data_container
        


def pie_chart_main(data_container):
    def get_browser_from_user_agent(series):
        """Extracts browser name from user agent strings.
        removes check_http entries as we only want browser name"""

        def remove_non_browser_ua(series):
            indicators = ['check_http','-','python-requests', 'go-http-client', 'chrome privacy']
            for indicator in indicators:
                series = series[~series.str.lower().str.contains(indicator)]
            return series
            

        def extract_browser(ua):
            ua = str(ua).lower()
            if 'bot' in ua or 'crawl' in ua or 'spider' in ua or 'bytedance' in ua or 'googleother' in ua:
                return 'Bot'
            if 'firefox' in ua:
                return 'Firefox'
            elif 'safari' in ua and 'chrome' not in ua:
                return 'Safari'
            elif 'edge' in ua or 'edg' in ua or 'edga' in ua:
                return 'Edge'
            elif 'opera' in ua or 'opr' in ua:
                return 'Opera'
            elif 'chrome' in ua:
                #regex for only chrome browser, not chromium based browsers
                #the pattern looks for chrome followed by a slash and version number, immediately followed by safari and version number
                chrome_pattern = r'chrome\/[0-9]+.*safari\/[0-9]+'
                if re.search(chrome_pattern, ua):
                    #logger.debug(f"Extracted Chrome from user agent: {ua}")
                    return 'Chrome'
                else:
                    logger.debug(f"Chromium based browser detected, categorizing as Other: {ua}")
                    return 'Other'
                
            else:
                logger.debug(f"Browser could not be determined from user agent: {ua}")
                return 'Other'
        series = remove_non_browser_ua(series)
        return series.apply(extract_browser)

    columns_to_visualise = [['status', 'group_small'],['status', 'ignore_largest_2'], ['request_type'], ['user_agent', ['title','Browsers and Bots'], ['modify_data',get_browser_from_user_agent],'group_small', 'ignore_largest']]
    visualiser = PieChartVisualiser(data_container.processed, columns_to_visualise)
    visualiser.loop_over_columns()

def bar_chart_main(data_container):
    def modify_user_agent(series):
        """Custom modifier to simplify user agent strings."""
        def simplify_ua(ua):
            new_ua = ""
            ua = str(ua).lower()
            if 'firefox' in ua:
                new_ua += 'firefox'
                logger.debug("Detected Firefox in user agent")
            elif 'safari' in ua and 'chrome' not in ua:
                new_ua += 'safari'
                logger.debug("Detected Safari in user agent")
            elif 'edge' in ua:
                new_ua += 'edge'
                logger.debug("Detected Edge in user agent")
            elif 'opera' in ua or 'opr' in ua:
                new_ua += 'opera'
                logger.debug("Detected Opera in user agent")
            elif 'gpt' in ua.lower():
                logger.debug("Detected GPT in user agent")
                new_ua += 'gpt-agent'
            elif 'google' in ua:
                new_ua += 'googlebot'
                logger.debug("Detected Googlebot in user agent")
            elif 'bing' in ua:
                new_ua += 'bing'
                logger.debug("Detected Bing in user agent")
            elif 'ahref' in ua:
                new_ua += 'ahref'
                logger.debug("Detected Ahref in user agent")
            elif 'mj12' in ua:
                new_ua += 'majestic'
                logger.debug("Detected Majestic in user agent")
            elif 'semrush' in ua:
                new_ua += 'semrush'
                logger.debug("Detected Semrush in user agent")
            elif 'bytedance' in ua or 'baiduspider' in ua:
                new_ua += 'baidu bot'
                logger.debug("Detected Baidu in user agent")
            elif 'vivaldi' in ua:
                new_ua += 'vivaldi'
                logger.debug("Detected Vivaldi in user agent")
            elif 'edg' in ua or 'edga' in ua:
                new_ua += 'edge'
                logger.debug("Detected Edge in user agent")
            elif 'chrome' in ua and 'bot' not in ua:
                new_ua += 'chrome'
                logger.debug("Detected Chrome in user agent")
                logger.debug(f"User agent checked: {ua}")
            else:
                logger.debug(f"User agent did not match any known categories")
                if 'bot' in ua:
                    logger.debug("Detected bot in user agent")
                    ua = 'Miscellaneous bots'
                return ua
            
            if 'bot' in ua:
                logger.debug("Detected bot in user agent")
                new_ua += ' bot'

            return new_ua if new_ua else ua
        return series.apply(simplify_ua)

    columns_to_visualise = [['referrer', 'ignore_largest_2'], ['user_agent',['modify_data',modify_user_agent],'group_smaller', 'ignore_largest'], ['ip','group_small']]
    visualiser = BarChartVisualiser(data_container.processed, columns_to_visualise)
    visualiser.loop_over_columns()

def line_chart_main(data_container):
    def convert_datetime(series):
        #datetime is currently in format DD/MMM/YYYY:HH:MM:SS +ZZZZ
        #we want to convert to pandas datetime and floor to the hour
        pattern = r'(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) \+(\d{4})'
        def parse_datetime(dt_str):
            match = re.match(pattern, dt_str)
            if match:
                day, month_str, year, hour, minute, second, tz = match.groups()
                month = pd.to_datetime(month_str, format='%b').month

                minute = int(minute) // 10 * 10  # Floor to nearest 10 minutes
                logger.debug(f"Parsed datetime string: {dt_str} to {year}-{month}-{day} {hour}:{minute}")
                return pd.Timestamp(year=int(year), month=month, day=int(day), hour=int(hour), minute=minute)
            else:
                logger.warning(f"Date string did not match expected format: {dt_str}")
                return pd.NaT
        return series.apply(parse_datetime).dt.floor('h')
    
    def floor_counts_above_threshold(series, threshold=10):
        counts = series.value_counts()
        to_floor = counts[counts > threshold].index
        logger.debug(f"Flooring counts above threshold {threshold} for values: {to_floor.tolist()}")
        return series.apply(lambda x: x if x not in to_floor else pd.NaT)

    columns_to_visualise = [['datetime',['modify_data',convert_datetime]]]
    visualiser = line_chart_visualiser(data_container.processed, columns_to_visualise)
    visualiser.loop_over_columns()


def main():
    data_container = loadData()
    logger.info("Data loaded successfully")

    pie_chart_main(data_container)
    #bar_chart_main(data_container)
    #line_chart_main(data_container)

logger.info("Application started")
try:
    main()
    logger.info("Application finished successfully\n")
except Exception as e:
    logger.error(f"Application encountered an error: {e}")