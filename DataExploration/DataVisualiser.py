import pandas as pd
import matplotlib.pyplot as plt
import logging
import re
logger = logging.getLogger(__name__)

class Visualiser:
    """Base class for data visualisation.
    Attributes:
        data (pd.DataFrame): The processed data to visualise.
        columns (list): List of columns available for visualisation with modifiers.
    Methods:
        __init__(self, data, columns): Initializes the Visualiser with processed data.
        ignore_largest(self, series): Ignores the largest category in a pandas Series.
        group_small(self, series, threshold=0.01): Groups small categories in a pandas Series into 'Other'.
        apply_modifiers(self, column, modifiers): Applies modifiers to the data for visualisation.
        plot_column(self, column, modifiers=[]): Placeholder method for plotting a column.
        loop_over_columns(self): Loops over available columns to create visualisations.
    """
    data = None
    columns = []

    def __init__(self, data, columns):
        """Initializes the Visualiser with selected columns
        Args:
            data (pd.DataFrame): The processed data to visualise.
            columns (list): List of columns available for visualisation with modifiers.
        """
        logger.info("Initializing Visualiser")
        self.data = data
        self.columns = columns
        logger.debug("Available columns for visualisation: %s", self.columns)

    def ignore_largest(self, series):
        """Ignores the largest category in a pandas Series.
        args:
            series (pd.Series): The data series to modify.
        Returns:
            pd.Series: The modified series with the largest category removed.
        """
        largest_value = series.value_counts().idxmax()
        logger.debug("Ignoring largest category: %s", largest_value)
        return series[series != largest_value]
    
    def group_small(self, series, threshold=0.01):
        """Groups small categories in a pandas Series into 'Other'.
        args:
            series (pd.Series): The data series to modify.
            threshold (float): The frequency threshold below which categories are grouped.
        Returns:
            pd.Series: The modified series with small categories grouped.
        """
        value_counts = series.value_counts(normalize=True)
        small_categories = value_counts[value_counts < threshold].index
        logger.debug("Grouping small categories: %s", small_categories.tolist())
        return series.apply(lambda x: 'Other' if x in small_categories else x)
    
    def group_smaller(self, series, threshold=0.005):
        return self.group_small(series, threshold)

    def apply_modifiers(self, column, modifiers):
        """Applies modifiers to the data for visualisation.
        args:
            modifiers (list): List of modifiers to apply.
        Returns:
            pd.DataFrame: The modified data.
        """
        modified_data = self.data.copy()
        for modifier in modifiers:
            if modifier == "ignore_largest":
                logger.debug("Applying modifier: ignore_largest to column: %s", column)
                modified_data[column] = self.ignore_largest(modified_data[column])
            elif modifier == "ignore_largest_2":
                logger.debug("Applying modifier: ignore_largest_2 to column: %s", column)
                modified_data[column] = self.ignore_largest(self.ignore_largest(modified_data[column]))
            elif modifier == "group_small":
                logger.debug("Applying modifier: group_small to column: %s", column)
                modified_data[column] = self.group_small(modified_data[column])
            elif modifier == "group_smaller":
                logger.debug("Applying modifier: group_smaller to column: %s", column)
                modified_data[column] = self.group_smaller(modified_data[column])
            elif modifier[0] == "modify_data":
                logger.debug("Applying custom modifier to column: %s with function %s", column, str(modifier[1]))
                try:
                    func = modifier[1]
                    modified_data[column] = func(modified_data[column])
                except Exception as e:
                    logger.error("Error applying custom modifier: %s", e)
                    logger.warning("Skipping custom modifier due to error.")
            elif modifier[0] == 'title':
                pass
            else:
                logger.warning("Unknown modifier: %s", modifier)
        return modified_data[column]


    def plot_column(self, column, modifiers=[]):
        """Placeholder method for plotting a column.
        args:
            column (str): The column to visualise.
            modifiers (list): List of modifiers to adjust the visualisation.
        """
        logger.warning("plot_column method not implemented in base Visualiser class")


    def loop_over_columns(self):
        """Loops over available columns to create visualisations."""
        logger.debug("Starting to loop over columns for visualisation")
        for column in self.columns:
            logger.info("Visualising column: %s", column[0])
            if len(column) == 1:
                self.plot_column(column[0])
            else:
                self.plot_column(column[0], modifiers=column[1:])


class PieChartVisualiser(Visualiser):
    """Class to create pie chart visualisations from selected columns of processed data.
    Attributes:
        data (pd.DataFrame): The processed data to visualise.
        columns (list): List of columns available for visualisation.
    Methods:
        __init__(self, data, columns): Initializes the PieChartVisualiser with processed data.
        plot_column(self, column, modifiers=[]): Plots a pie chart for the specified column.
        loop_over_columns(self): Loops over available columns to create pie charts.

    """    


    def plot_column(self, column, modifiers=[]):
        """Plots a pie chart for the specified column.
        args:
            column (str): The column to visualise.
            modifiers (list): List of modifiers to adjust the pie chart (e.g., ignore_largest, group_small).
        """
        if column not in self.data.columns:
            logger.error("Column '%s' not found in data", column)
            raise ValueError(f"Column '{column}' not found in data.")
        
        # Apply modifiers
        title = None
        if len(modifiers) > 0:
            modified_series = self.apply_modifiers(column, modifiers)
            value_counts = modified_series.value_counts()
        
        else:
            value_counts = self.data[column].value_counts()

        plt.figure(figsize=(10, 10))
        plt.pie(value_counts, labels=value_counts.index, autopct='%1.1f%%', startangle=140, pctdistance=0.85)
        modifiers_str = ""
        for modifier in modifiers:
            modifiers_str += f"{modifier} "
            if modifier[0] == 'title':
                title = modifier[1]
        
        if title:
            plt.title(title)
        else:
            plt.title(f'{column} with modifiers: {modifiers_str}') if modifiers else plt.title(f'{column} with modifiers: None')
        plt.legend(value_counts.index, loc='center left', bbox_to_anchor=(1, 0, 0.5, 1))
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        plt.show()
        logger.info("Pie chart plotted for column: %s", column)

class BarChartVisualiser(Visualiser):
    """Class to create bar chart visualisations from selected columns of processed data.
    Attributes:
        data (pd.DataFrame): The processed data to visualise.
        columns (list): List of columns available for visualisation.
    Methods:
        __init__(self, data, columns): Initializes the BarChartVisualiser with processed data.
        plot_column(self, column, modifiers=[]): Plots a bar chart for the specified column.
        loop_over_columns(self): Loops over available columns to create bar charts.
    """

    def plot_column(self, column, modifiers=[]):
        """Plots a bar chart for the specified column.
        args:
            column (str): The column to visualise.
            modifiers (list): List of modifiers to adjust the bar chart (e.g., ignore_largest, group_small).
        """
        if column not in self.data.columns:
            logger.error("Column '%s' not found in data", column)
            raise ValueError(f"Column '{column}' not found in data.")
        
        # Apply modifiers
        if len(modifiers) > 0:
            modified_series = self.apply_modifiers(column, modifiers)
            value_counts = modified_series.value_counts()
        
        else:
            value_counts = self.data[column].value_counts()
        
        # if length of unique value is longer than 20 characters, add line breaks to the labels

        title = None
        plt.figure(figsize=(12, 6))
        value_counts.plot(kind='barh')
        modifiers_str = ""
        for modifier in modifiers:
            modifiers_str += f"{modifier} "
            if modifier[0] == 'title':
                title = modifier[1]
        
        if title:
            plt.title(title)
        else:
            plt.title(f'{column} with modifiers: {modifiers_str}') if modifiers else plt.title(f'{column} with modifiers: None')

        plt.xlabel('Count')
        plt.ylabel(column)
        plt.subplots_adjust(left=0.3) # Adjust left to accommodate long labels
        plt.show()
        logger.info("Bar chart plotted for column: %s", column)

class line_chart_visualiser(Visualiser):
    def plot_column(self, column, modifiers=[]):
        """Plots a line chart for the specified column.
        args:
            column (str): The column to visualise.
            modifiers (list): List of modifiers to adjust the line chart (e.g., ignore_largest, group_small).
        """
        if column not in self.data.columns:
            logger.error("Column '%s' not found in data", column)
            raise ValueError(f"Column '{column}' not found in data.")
        
        # Apply modifiers
        if len(modifiers) > 0:
            modified_series = self.apply_modifiers(column, modifiers)
            value_counts = modified_series.value_counts().sort_index()
        
        else:
            value_counts = self.data[column].value_counts().sort_index()
        
        title = None
        plt.figure(figsize=(12, 6))
        value_counts.plot(kind='line', marker='o')
        modifiers_str = ""
        for modifier in modifiers:
            modifiers_str += f"{modifier} "
            if modifier[0] == 'title':
                title = modifier[1]
        
        if title:
            plt.title(title)
        else:
            plt.title(f'{column} with modifiers: {modifiers_str}') if modifiers else plt.title(f'{column} with modifiers: None')

        plt.xlabel(column)
        #rotate x labels for better readability
        plt.xticks(rotation=45)
        plt.ylabel('Count')
        plt.grid(True)
        plt.show()
        logger.info("Line chart plotted for column: %s", column)


if __name__ == "__main__":
    from sys import path
    import os

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

    def pie_chart_main():
        columns_to_visualise = [['status', 'group_small'],['status', 'ignore_largest_2'], ['request_type', 'ignore_largest'], ['user_agent', ['title','Browsers and Bots'], ['modify_data',get_browser_from_user_agent],'group_small']]
        visualiser = PieChartVisualiser(data_container.processed, columns_to_visualise)
        visualiser.loop_over_columns()

    def bar_chart_main():
        columns_to_visualise = [['referrer', 'ignore_largest_2'], ['user_agent',['modify_data',modify_user_agent],'group_smaller', 'ignore_largest'], ['ip','group_small']]
        visualiser = BarChartVisualiser(data_container.processed, columns_to_visualise)
        visualiser.loop_over_columns()

    def line_chart_main():
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
    os.makedirs('SelfLogs/DataExplorationLogs', exist_ok=True)
    logging.basicConfig(filename='SelfLogs/DataExplorationLogs/data_visualiser.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logger.addHandler(logging.StreamHandler())# Add a stream handler to output logs to console
    logger.debug("Starting DataVisualiser module as main program")

    # set file location relative to this file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    processor_directory = os.path.join(current_dir, '..', 'DataProcessing')
    path.append(processor_directory) # Add DataProcessing directory to sys.path to allow import

    from DataLoader import DataContainer
    logger.debug("DataLoader imported successfully")
    data_container = DataContainer()
    logger.debug("DataContainer instance created in DataVisualiser")
    data_container.process_data()
    logger.info("Data processed in DataVisualiser")

    #pie_chart_main()
    #bar_chart_main()
    line_chart_main()