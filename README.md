# Web Scraping Practice Project With NFL Salary Cap Data from Spotrac
## Overview
I practiced web scraping NFL salary cap data from [Spotrac](https://www.spotrac.com/). The ETL process is shown in the below diagram and is described in the below steps.


![image](https://github.com/csim3/nfl-salary-cap/assets/79472629/595ee93f-0d12-4636-981e-84569246a452)

## Steps
1. Used Beautiful Soup to parse the HTML of each NFL team's 2023 salary cap table from Spotrac.
2. Produced Pandas DataFrame objects to upload salary cap data to local MySQL database.
3. Loaded data from MySQL to Google Sheets. Google Sheets was specifically used since it is a free, web-based program whose data is automatically refreshed on a daily basis in Tableau Public dashboards.
4. Constructed a Tableau dashboard that is viewable on [Tableau Public](https://public.tableau.com/views/NFLSalaryCapTracker/Dashboard1?:language=en-US&:display_count=n&:origin=viz_share_link) to visualize salary cap summaries of each team and of the league.

## Tableau Dashboard Screenshot

![image](https://github.com/csim3/nfl-salary-cap/assets/79472629/397c4f56-2460-4a30-b6b7-49f5a4db06e6)