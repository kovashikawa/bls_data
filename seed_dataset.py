"""
Seed dataset for BLS Data Agent distillation.
70 natural language questions mapped to correct MCP tool calls.
Covers all 7 tools with diverse economic queries.
"""

SEED_DATA = [
    # ── get_series (25 examples) ──
    {
        "question": "What's the trend in CPI All Items from 2020 to 2024?",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SA0", "start": "2020", "end": "2024"},
    },
    {
        "question": "Show me food price inflation since 2021.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAF1", "start": "2021"},
    },
    {
        "question": "How have energy prices changed over the last 3 years?",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SA0E", "start": "2022"},
    },
    {
        "question": "Get the unemployment rate data from 2019 to 2024.",
        "tool": "get_series",
        "arguments": {"series_id": "LNS14000000", "start": "2019", "end": "2024"},
    },
    {
        "question": "What's the current employment level in the US?",
        "tool": "get_series",
        "arguments": {"series_id": "CES0000000001"},
    },
    {
        "question": "Show me housing CPI from 2022 to present.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAH1", "start": "2022"},
    },
    {
        "question": "How much did medical care costs increase since 2020?",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAM1", "start": "2020"},
    },
    {
        "question": "Give me the CPI for transportation over the last 2 years.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAT1", "start": "2023"},
    },
    {
        "question": "What was apparel CPI from 2021 through 2023?",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAA1", "start": "2021", "end": "2023"},
    },
    {
        "question": "Show me the CPI for all items less food and energy for the last 5 years.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SA0L1E", "start": "2020"},
    },
    {
        "question": "What's the labor force participation rate been since 2019?",
        "tool": "get_series",
        "arguments": {"series_id": "LNS11300000", "start": "2019"},
    },
    {
        "question": "Show me the employment-population ratio trend.",
        "tool": "get_series",
        "arguments": {"series_id": "LNS12300000", "start": "2020"},
    },
    {
        "question": "What's been happening with average hourly earnings?",
        "tool": "get_series",
        "arguments": {"series_id": "CES0500000003", "start": "2022"},
    },
    {
        "question": "Show me CPI for education and communication since 2020.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAE1", "start": "2020"},
    },
    {
        "question": "How have recreation prices changed in the last 3 years?",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAR1", "start": "2022"},
    },
    {
        "question": "Give me the commodities CPI from 2021 to 2024.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAC1", "start": "2021", "end": "2024"},
    },
    {
        "question": "What was the CPI for services in 2023?",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAS1", "start": "2023", "end": "2023"},
    },
    {
        "question": "Show me the CPI for food at home for the past 4 years.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SAF11", "start": "2021"},
    },
    {
        "question": "Get data on food away from home inflation since 2020.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SEFV01", "start": "2020"},
    },
    {
        "question": "What's the CPI for new vehicles over the last 3 years?",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SETA01", "start": "2022"},
    },
    {
        "question": "Show me used cars and trucks CPI since 2021.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SETA02", "start": "2021"},
    },
    {
        "question": "How has gasoline CPI changed from 2020 to 2024?",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SETB01", "start": "2020", "end": "2024"},
    },
    {
        "question": "Show me electricity prices CPI for the last 4 years.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SEHF01", "start": "2021"},
    },
    {
        "question": "What's the rent of primary residence CPI since 2020?",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SEHA01", "start": "2020"},
    },
    {
        "question": "Show me owners equivalent rent CPI trend.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SEHC01", "start": "2021"},
    },

    # ── list_surveys (5 examples) ──
    {
        "question": "What BLS surveys are available?",
        "tool": "list_surveys",
        "arguments": {},
    },
    {
        "question": "List all the economic data surveys from the BLS.",
        "tool": "list_surveys",
        "arguments": {},
    },
    {
        "question": "What kinds of data does the Bureau of Labor Statistics publish?",
        "tool": "list_surveys",
        "arguments": {},
    },
    {
        "question": "Show me all available survey programs at BLS.",
        "tool": "list_surveys",
        "arguments": {},
    },
    {
        "question": "What surveys can I query through the BLS API?",
        "tool": "list_surveys",
        "arguments": {},
    },

    # ── popular_series (8 examples) ──
    {
        "question": "What are the most popular CPI series?",
        "tool": "popular_series",
        "arguments": {"survey": "CU"},
    },
    {
        "question": "Show me the most requested employment data series.",
        "tool": "popular_series",
        "arguments": {"survey": "CE"},
    },
    {
        "question": "What are the popular unemployment-related series?",
        "tool": "popular_series",
        "arguments": {"survey": "LN"},
    },
    {
        "question": "List the top PPI series.",
        "tool": "popular_series",
        "arguments": {"survey": "PC"},
    },
    {
        "question": "Show me popular productivity series.",
        "tool": "popular_series",
        "arguments": {"survey": "PR"},
    },
    {
        "question": "What are the most commonly requested JOLTS series?",
        "tool": "popular_series",
        "arguments": {"survey": "JT"},
    },
    {
        "question": "What are the overall most popular BLS series?",
        "tool": "popular_series",
        "arguments": {},
    },
    {
        "question": "Show popular wage and earnings data series.",
        "tool": "popular_series",
        "arguments": {"survey": "LE"},
    },

    # ── search_series (10 examples) ──
    {
        "question": "Search for CPI series related to dairy products.",
        "tool": "search_series",
        "arguments": {"query": "dairy"},
    },
    {
        "question": "Find BLS series about airline fares.",
        "tool": "search_series",
        "arguments": {"query": "airline"},
    },
    {
        "question": "What CPI series cover prescription drugs?",
        "tool": "search_series",
        "arguments": {"query": "prescription"},
    },
    {
        "question": "Search for series about college tuition.",
        "tool": "search_series",
        "arguments": {"query": "tuition"},
    },
    {
        "question": "Find CPI data related to beef and meat prices.",
        "tool": "search_series",
        "arguments": {"query": "beef"},
    },
    {
        "question": "What series cover alcoholic beverages?",
        "tool": "search_series",
        "arguments": {"query": "alcoholic beverages", "limit": 10},
    },
    {
        "question": "Search for CPI series about internet services.",
        "tool": "search_series",
        "arguments": {"query": "internet"},
    },
    {
        "question": "Find data about hotel and motel prices.",
        "tool": "search_series",
        "arguments": {"query": "hotel"},
    },
    {
        "question": "What CPI series track tobacco products?",
        "tool": "search_series",
        "arguments": {"query": "tobacco"},
    },
    {
        "question": "Search for series about childcare and baby food.",
        "tool": "search_series",
        "arguments": {"query": "baby food", "limit": 5},
    },

    # ── get_series_info (8 examples) ──
    {
        "question": "What metadata is available for CPI All Items?",
        "tool": "get_series_info",
        "arguments": {"series_id": "CUUR0000SA0"},
    },
    {
        "question": "Tell me about the unemployment rate series.",
        "tool": "get_series_info",
        "arguments": {"series_id": "LNS14000000"},
    },
    {
        "question": "Get info on the total nonfarm employment series.",
        "tool": "get_series_info",
        "arguments": {"series_id": "CES0000000001"},
    },
    {
        "question": "What survey and measure type is CPI for energy?",
        "tool": "get_series_info",
        "arguments": {"series_id": "CUUR0000SETG01"},
    },
    {
        "question": "Get metadata for the food CPI series.",
        "tool": "get_series_info",
        "arguments": {"series_id": "CUUR0000SAF1"},
    },
    {
        "question": "What's the seasonality of the housing CPI series?",
        "tool": "get_series_info",
        "arguments": {"series_id": "CUUR0000SAH1"},
    },
    {
        "question": "Get series info for the labor force participation rate.",
        "tool": "get_series_info",
        "arguments": {"series_id": "LNS11300000"},
    },
    {
        "question": "Tell me about the average hourly earnings series.",
        "tool": "get_series_info",
        "arguments": {"series_id": "CES0500000003"},
    },

    # ── analyze_cpi_seasonality (8 examples) ──
    {
        "question": "Analyze the seasonality of overall CPI.",
        "tool": "analyze_cpi_seasonality",
        "arguments": {"series_id": "CUUR0000SA0"},
    },
    {
        "question": "Show me the seasonal pattern in food prices.",
        "tool": "analyze_cpi_seasonality",
        "arguments": {"series_id": "CUUR0000SAF1"},
    },
    {
        "question": "What's the seasonal pattern for energy CPI?",
        "tool": "analyze_cpi_seasonality",
        "arguments": {"series_id": "CUUR0000SETG01"},
    },
    {
        "question": "Do housing costs have seasonal variation?",
        "tool": "analyze_cpi_seasonality",
        "arguments": {"series_id": "CUUR0000SAH1"},
    },
    {
        "question": "Analyze seasonality in gasoline prices.",
        "tool": "analyze_cpi_seasonality",
        "arguments": {"series_id": "CUUR0000SETB01"},
    },
    {
        "question": "What seasonal patterns exist in apparel CPI?",
        "tool": "analyze_cpi_seasonality",
        "arguments": {"series_id": "CUUR0000SAA1"},
    },
    {
        "question": "Analyze the seasonal variation in transportation costs.",
        "tool": "analyze_cpi_seasonality",
        "arguments": {"series_id": "CUUR0000SAT1"},
    },
    {
        "question": "Show me the seasonal pattern for food at home.",
        "tool": "analyze_cpi_seasonality",
        "arguments": {"series_id": "CUUR0000SAF11"},
    },

    # ── Multi-step / compound (6 examples) ──
    {
        "question": "First list the surveys, then show me popular employment series.",
        "tool": "list_surveys",
        "arguments": {},
        "next_tool": "popular_series",
        "next_arguments": {"survey": "CE"},
    },
    {
        "question": "Search for rent-related series, then get CPI data for the first result.",
        "tool": "search_series",
        "arguments": {"query": "rent of primary residence", "limit": 3},
        "next_tool": "get_series",
        "next_arguments": {"series_id": "CUUR0000SEHA01"},
    },
    {
        "question": "List popular CPI series, then get the metadata for the top one.",
        "tool": "popular_series",
        "arguments": {"survey": "CU"},
        "next_tool": "get_series_info",
        "next_arguments": {"series_id": "CUUR0000SA0"},
    },
    {
        "question": "Search for healthcare series and analyze the seasonality of the main one.",
        "tool": "search_series",
        "arguments": {"query": "medical care", "limit": 5},
        "next_tool": "analyze_cpi_seasonality",
        "next_arguments": {"series_id": "CUUR0000SAM1"},
    },
    {
        "question": "Get CPI all items and unemployment rate data for 2024.",
        "tool": "get_series",
        "arguments": {"series_id": "CUUR0000SA0", "start": "2024", "end": "2024"},
        "next_tool": "get_series",
        "next_arguments": {"series_id": "LNS14000000", "start": "2024", "end": "2024"},
    },
    {
        "question": "Find and get the CPI for fuel oil, then analyze its seasonality.",
        "tool": "search_series",
        "arguments": {"query": "fuel oil", "limit": 3},
        "next_tool": "analyze_cpi_seasonality",
        "next_arguments": {"series_id": "CUUR0000SEHE01"},
    },
]

if __name__ == "__main__":
    import json, sys
    json.dump(SEED_DATA, sys.stdout, indent=2)
    print(f"\n// {len(SEED_DATA)} seed examples", file=sys.stderr)
