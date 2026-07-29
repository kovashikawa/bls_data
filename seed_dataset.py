"""
Seed dataset for BLS Data Agent distillation.

Organised as concept tables rather than a flat list, because the thing the model
has to learn is a *lookup*: "medical care" -> CUUR0000SAM. Two consequences:

1. Every concept carries several distinct phrasings. The previous version had
   ~1 question per concept, which gave the model nothing to generalise from —
   entity accuracy sat at 13%.
2. Splits hold out PHRASINGS, not concepts (see build_dataset.py). Holding out a
   whole concept asks the model to invent a series id it has never seen; 4 of 11
   scored test items were unanswerable that way.

Every series id below is verified against the bundled CPI master catalog at
build time by build_dataset._assert_ids_exist().

Relative time phrasings ("over the last 3 years") are deliberately avoided.
They made the label depend on the current date, and the old expander produced
rows whose question said "since 2020" while the arguments said 2018-2022.
Questions here either name explicit years, or name none and carry no date args.
"""

# ── get_series: series_id -> [(question, extra_args), ...] ──
# extra_args must agree with the question text. No year in the text => no dates.
GET_SERIES = {
    "CUUR0000SA0": [
        ("What's the trend in CPI All Items from 2020 to 2024?", {"start": "2020", "end": "2024"}),
        ("Show me headline CPI since 2021.", {"start": "2021"}),
        ("Pull the all-items consumer price index.", {}),
    ],
    "CUUR0000SA0E": [
        ("How have energy prices changed from 2020 to 2025?", {"start": "2020", "end": "2025"}),
        ("Get the CPI for energy since 2019.", {"start": "2019"}),
        ("What is energy inflation running at?", {}),
    ],
    "CUUR0000SA0L1E": [
        ("Show me core CPI from 2019 to 2024.", {"start": "2019", "end": "2024"}),
        ("What's CPI excluding food and energy since 2020?", {"start": "2020"}),
        ("Give me the all items less food and energy index.", {}),
    ],
    "CUUR0000SAF1": [
        ("Show me food price inflation since 2021.", {"start": "2021"}),
        ("What did food CPI do between 2018 and 2023?", {"start": "2018", "end": "2023"}),
        ("Get the food consumer price index.", {}),
    ],
    "CUUR0000SAF11": [
        ("What's the CPI for food at home since 2021?", {"start": "2021"}),
        ("How have grocery prices moved from 2020 to 2025?", {"start": "2020", "end": "2025"}),
        ("Pull the food at home index.", {}),
    ],
    "CUUR0000SEFV": [
        ("Get data on food away from home inflation since 2020.", {"start": "2020"}),
        ("What have restaurant prices done from 2021 to 2024?", {"start": "2021", "end": "2024"}),
        ("Show me the food away from home CPI.", {}),
    ],
    "CUUR0000SAF116": [
        ("What's the CPI for alcoholic beverages since 2020?", {"start": "2020"}),
        ("Show me alcohol price inflation from 2019 to 2024.", {"start": "2019", "end": "2024"}),
        ("Get the alcoholic beverages index.", {}),
    ],
    "CUUR0000SAH": [
        ("Show me housing CPI since 2022.", {"start": "2022"}),
        ("What did housing costs do from 2019 to 2024?", {"start": "2019", "end": "2024"}),
        ("Get the housing consumer price index.", {}),
    ],
    "CUUR0000SAH1": [
        ("What's the shelter CPI since 2021?", {"start": "2021"}),
        ("Show me shelter inflation from 2020 to 2025.", {"start": "2020", "end": "2025"}),
        ("Pull the shelter index.", {}),
    ],
    "CUUR0000SEHA": [
        ("What's the rent of primary residence CPI since 2020?", {"start": "2020"}),
        ("How has rent inflation moved from 2021 to 2025?", {"start": "2021", "end": "2025"}),
        ("Get the primary residence rent index.", {}),
    ],
    "CUUR0000SEHC01": [
        ("Show me owners equivalent rent since 2021.", {"start": "2021"}),
        ("What did OER do between 2019 and 2024?", {"start": "2019", "end": "2024"}),
        ("Get the owners' equivalent rent index.", {}),
    ],
    "CUUR0000SEHF01": [
        ("Show me electricity price CPI since 2021.", {"start": "2021"}),
        ("What have electricity prices done from 2020 to 2025?", {"start": "2020", "end": "2025"}),
        ("Get the electricity index.", {}),
    ],
    "CUUR0000SEHF02": [
        ("What's the CPI for piped gas service since 2020?", {"start": "2020"}),
        ("Show me natural gas utility prices from 2021 to 2024.", {"start": "2021", "end": "2024"}),
        ("Pull the utility gas service index.", {}),
    ],
    "CUUR0000SEHE01": [
        ("How has fuel oil CPI moved since 2020?", {"start": "2020"}),
        ("Show me heating oil prices from 2019 to 2023.", {"start": "2019", "end": "2023"}),
        ("Get the fuel oil index.", {}),
    ],
    "CUUR0000SAM": [
        ("How much did medical care costs increase since 2020?", {"start": "2020"}),
        ("Show me healthcare CPI from 2019 to 2024.", {"start": "2019", "end": "2024"}),
        ("Get the medical care consumer price index.", {}),
    ],
    "CUUR0000SEMF01": [
        ("What's the CPI for prescription drugs since 2020?", {"start": "2020"}),
        ("How have prescription drug prices moved from 2018 to 2023?", {"start": "2018", "end": "2023"}),
        ("Pull the prescription drugs index.", {}),
    ],
    "CUUR0000SEMD": [
        ("Show me hospital services CPI since 2021.", {"start": "2021"}),
        ("What did hospital costs do from 2020 to 2025?", {"start": "2020", "end": "2025"}),
        ("Get the hospital and related services index.", {}),
    ],
    "CUUR0000SAT": [
        ("Give me the CPI for transportation since 2023.", {"start": "2023"}),
        ("What did transportation costs do from 2020 to 2024?", {"start": "2020", "end": "2024"}),
        ("Get the transportation consumer price index.", {}),
    ],
    "CUUR0000SETA01": [
        ("What's the CPI for new vehicles since 2022.", {"start": "2022"}),
        ("Show me new car prices from 2019 to 2024.", {"start": "2019", "end": "2024"}),
        ("Get the new vehicles index.", {}),
    ],
    "CUUR0000SETA02": [
        ("Show me used cars and trucks CPI since 2021.", {"start": "2021"}),
        ("What did used car prices do from 2020 to 2025?", {"start": "2020", "end": "2025"}),
        ("Pull the used cars and trucks index.", {}),
    ],
    "CUUR0000SETB01": [
        ("How has gasoline CPI changed from 2020 to 2024?", {"start": "2020", "end": "2024"}),
        ("Show me gas prices since 2021.", {"start": "2021"}),
        ("Get the gasoline all types index.", {}),
    ],
    "CUUR0000SETG01": [
        ("What have airline fares done since 2021?", {"start": "2021"}),
        ("Show me airfare CPI from 2019 to 2024.", {"start": "2019", "end": "2024"}),
        ("Get the airline fares index.", {}),
    ],
    "CUUR0000SAA": [
        ("What was apparel CPI from 2021 through 2023?", {"start": "2021", "end": "2023"}),
        ("Show me clothing price inflation since 2020.", {"start": "2020"}),
        ("Get the apparel consumer price index.", {}),
    ],
    "CUUR0000SAE": [
        ("Show me CPI for education and communication since 2020.", {"start": "2020"}),
        ("What did education and communication prices do from 2019 to 2024?", {"start": "2019", "end": "2024"}),
        ("Get the education and communication index.", {}),
    ],
    "CUUR0000SEEB": [
        ("What's happened to college tuition costs since 2020?", {"start": "2020"}),
        ("Show me tuition and childcare CPI from 2018 to 2023.", {"start": "2018", "end": "2023"}),
        ("Get the tuition, other school fees, and childcare index.", {}),
    ],
    "CUUR0000SAR": [
        ("How have recreation prices changed since 2022?", {"start": "2022"}),
        ("Show me recreation CPI from 2019 to 2024.", {"start": "2019", "end": "2024"}),
        ("Get the recreation consumer price index.", {}),
    ],
    "CUUR0000SAC": [
        ("Give me the commodities CPI from 2021 to 2024.", {"start": "2021", "end": "2024"}),
        ("What have goods prices done since 2020?", {"start": "2020"}),
        ("Get the commodities index.", {}),
    ],
    "CUUR0000SAS": [
        ("What was the CPI for services in 2023?", {"start": "2023", "end": "2023"}),
        ("Show me services inflation since 2021.", {"start": "2021"}),
        ("Get the services consumer price index.", {}),
    ],
    "CUUR0000SAG1": [
        ("What's the CPI for personal care since 2020?", {"start": "2020"}),
        ("Show me personal care prices from 2019 to 2024.", {"start": "2019", "end": "2024"}),
        ("Get the personal care index.", {}),
    ],
    "CUUR0000SEGA": [
        ("How have tobacco prices moved since 2020?", {"start": "2020"}),
        ("Show me tobacco and smoking products CPI from 2018 to 2023.", {"start": "2018", "end": "2023"}),
        ("Get the tobacco and smoking products index.", {}),
    ],
    # ── Labor series ──
    "LNS14000000": [
        ("Get the unemployment rate data from 2019 to 2024.", {"start": "2019", "end": "2024"}),
        ("What's the unemployment rate been since 2021?", {"start": "2021"}),
        ("Pull the U-3 unemployment rate.", {}),
    ],
    "LNS11300000": [
        ("What's the labor force participation rate been since 2019?", {"start": "2019"}),
        ("Show me labor force participation from 2020 to 2025.", {"start": "2020", "end": "2025"}),
        ("Get the labor force participation rate.", {}),
    ],
    "LNS12300000": [
        ("Show me the employment-population ratio since 2020.", {"start": "2020"}),
        ("What did the employment to population ratio do from 2019 to 2024?", {"start": "2019", "end": "2024"}),
        ("Get the employment-population ratio.", {}),
    ],
    "CES0000000001": [
        ("What's the current employment level in the US?", {}),
        ("Show me total nonfarm payroll employment since 2021.", {"start": "2021"}),
        ("Get total nonfarm employment from 2019 to 2024.", {"start": "2019", "end": "2024"}),
    ],
    "CES0500000003": [
        ("What's been happening with average hourly earnings since 2022?", {"start": "2022"}),
        ("Show me average hourly earnings from 2020 to 2025.", {"start": "2020", "end": "2025"}),
        ("Get average hourly earnings for total private.", {}),
    ],
}

# ── get_series_info: series_id -> [question, ...] ──
GET_SERIES_INFO = {
    "CUUR0000SA0": ["What metadata is available for CPI All Items?",
                    "Describe the all-items CPI series."],
    "LNS14000000": ["Tell me about the unemployment rate series.",
                    "What are the catalog details for the unemployment rate?"],
    "CES0000000001": ["Get info on the total nonfarm employment series.",
                      "What does the CES total nonfarm series cover?"],
    "CUUR0000SA0E": ["What survey and measure type is CPI for energy?",
                     "Give me the metadata for the energy CPI series."],
    "CUUR0000SAF1": ["Get metadata for the food CPI series.",
                     "What are the catalog details of the food index?"],
    "CUUR0000SAH": ["Is the housing CPI series seasonally adjusted?",
                    "Show me the catalog record for housing CPI."],
    "CUUR0000SAM": ["What metadata does the medical care CPI series have?",
                    "Describe the medical care index."],
    "CUUR0000SEHA": ["Tell me about the rent of primary residence series.",
                     "What are the details of the primary residence rent index?"],
    "CUUR0000SETB01": ["Get series info for gasoline CPI.",
                       "What does the gasoline all types series measure?"],
    "LNS11300000": ["Get series info for the labor force participation rate.",
                    "Describe the labor force participation series."],
    "CES0500000003": ["Tell me about the average hourly earnings series.",
                      "What are the catalog details for average hourly earnings?"],
    "CUUR0000SAS": ["What metadata is there for the services CPI?",
                    "Describe the services index."],
}

# ── analyze_cpi_seasonality: series_id -> [question, ...] ──
ANALYZE_SEASONALITY = {
    "CUUR0000SA0": ["Analyze the seasonality of overall CPI.",
                    "What seasonal pattern does headline CPI show?"],
    "CUUR0000SAF1": ["Show me the seasonal pattern in food prices.",
                     "Analyze seasonality for the food CPI."],
    "CUUR0000SA0E": ["What's the seasonal pattern for energy CPI?",
                     "Analyze how energy prices vary by month."],
    "CUUR0000SAH": ["Do housing costs have seasonal variation?",
                    "Analyze the seasonality of housing CPI."],
    "CUUR0000SETB01": ["Analyze seasonality in gasoline prices.",
                       "Which months do gas prices peak in?"],
    "CUUR0000SAA": ["What seasonal patterns exist in apparel CPI?",
                    "Analyze the monthly seasonality of clothing prices."],
    "CUUR0000SAT": ["Analyze the seasonal variation in transportation costs.",
                    "Does transportation CPI have a seasonal cycle?"],
    "CUUR0000SAF11": ["Show me the seasonal pattern for food at home.",
                      "Analyze grocery price seasonality."],
    "CUUR0000SETG01": ["Analyze the seasonality of airline fares.",
                       "Which months are airfares highest?"],
    "CUUR0000SEHF01": ["Does electricity CPI vary seasonally?",
                       "Analyze the seasonal pattern in electricity prices."],
    "CUUR0000SAR": ["Analyze seasonality in recreation prices.",
                    "What's the monthly pattern for recreation CPI?"],
    "CUUR0000SEFV": ["Analyze the seasonality of food away from home.",
                     "Do restaurant prices show seasonal variation?"],
}

# ── search_series: query -> [(question, extra_args), ...] ──
SEARCH_SERIES = {
    "dairy": [("Search for CPI series related to dairy products.", {}),
              ("Find series about dairy.", {})],
    "airline": [("Find BLS series about airline fares.", {}),
                ("Search the catalog for airline series.", {})],
    "prescription": [("What CPI series cover prescription drugs?", {}),
                     ("Search for prescription series.", {})],
    "tuition": [("Search for series about college tuition.", {}),
                ("Find tuition-related CPI series.", {"limit": 10})],
    "beef": [("Find CPI data related to beef prices.", {}),
             ("Search the catalog for beef series.", {})],
    "alcoholic beverages": [("What series cover alcoholic beverages?", {"limit": 10}),
                            ("Search for alcoholic beverages series.", {})],
    "internet": [("Search for CPI series about internet services.", {}),
                 ("Find internet-related series.", {})],
    "hotel": [("Find data about hotel prices.", {}),
              ("Search the catalog for hotel series.", {})],
    "tobacco": [("What CPI series track tobacco products?", {}),
                ("Search for tobacco series.", {})],
    "baby food": [("Search for series about baby food.", {"limit": 5}),
                  ("Find baby food CPI series.", {})],
    "eggs": [("Find CPI series for eggs.", {}),
             ("Search the catalog for egg prices.", {})],
    "coffee": [("Search for coffee price series.", {}),
               ("What CPI series cover coffee?", {})],
    "fuel oil": [("Search for fuel oil series.", {}),
                 ("Find CPI data on fuel oil.", {"limit": 5})],
    "rent": [("Search for rent-related CPI series.", {}),
             ("Find series about rent.", {"limit": 10})],
}

# ── popular_series: survey -> [question, ...] ──
POPULAR_SERIES = {
    "CU": ["What are the most popular CPI series?",
           "Show me the most-requested consumer price index series."],
    "CE": ["Show me the most requested employment data series.",
           "What are the popular Current Employment Statistics series?"],
    "LN": ["What are the popular unemployment-related series?",
           "Show me the most-requested labor force series."],
    "PC": ["List the top PPI series.",
           "What are the most popular producer price index series?"],
    "PR": ["Show me popular productivity series.",
           "What productivity series get requested most?"],
    "JT": ["What are the most commonly requested JOLTS series?",
           "Show me popular job openings and turnover series."],
    "LE": ["Show popular wage and earnings data series.",
           "What are the most-requested weekly earnings series?"],
    None: ["What are the overall most popular BLS series?",
           "Show me the most-requested series across all of BLS."],
}

# ── list_surveys ──
LIST_SURVEYS = [
    "What BLS surveys are available?",
    "List all the economic data surveys from the BLS.",
    "What kinds of data does the Bureau of Labor Statistics publish?",
    "Show me all available survey programs at BLS.",
    "What surveys can I query through the BLS API?",
    "What BLS data categories exist?",
    "List the economic data programs at the Bureau of Labor Statistics.",
    "What types of economic data does BLS track?",
]


def _build():
    """Flatten the concept tables into SEED_DATA.

    Each seed carries a `concept` key so build_dataset can stratify the split by
    concept — guaranteeing every concept appears in train while holding out
    unseen phrasings for val/test.
    """
    seeds = []
    for sid, entries in GET_SERIES.items():
        for question, extra in entries:
            seeds.append({"question": question, "tool": "get_series",
                          "arguments": {"series_id": sid, **extra},
                          "concept": f"get_series:{sid}"})
    for sid, questions in GET_SERIES_INFO.items():
        for question in questions:
            seeds.append({"question": question, "tool": "get_series_info",
                          "arguments": {"series_id": sid},
                          "concept": f"get_series_info:{sid}"})
    for sid, questions in ANALYZE_SEASONALITY.items():
        for question in questions:
            seeds.append({"question": question, "tool": "analyze_cpi_seasonality",
                          "arguments": {"series_id": sid},
                          "concept": f"analyze_cpi_seasonality:{sid}"})
    for query, entries in SEARCH_SERIES.items():
        for question, extra in entries:
            seeds.append({"question": question, "tool": "search_series",
                          "arguments": {"query": query, **extra},
                          "concept": f"search_series:{query}"})
    for survey, questions in POPULAR_SERIES.items():
        for question in questions:
            seeds.append({"question": question, "tool": "popular_series",
                          "arguments": {} if survey is None else {"survey": survey},
                          "concept": f"popular_series:{survey}"})
    for question in LIST_SURVEYS:
        seeds.append({"question": question, "tool": "list_surveys",
                      "arguments": {}, "concept": "list_surveys"})

    questions = [s["question"] for s in seeds]
    if len(questions) != len(set(questions)):
        dupes = {q for q in questions if questions.count(q) > 1}
        raise ValueError(f"duplicate seed questions: {sorted(dupes)}")
    return seeds


# NOTE: the six compound "first do X, then do Y" seeds from the previous version
# are gone. format_training_example only ever emitted the first call, so their
# next_tool/next_arguments were silently discarded and the label answered half
# the question. Training on them taught the model to ignore part of a request.
# Multi-step calling needs a format that can express two calls; it was never
# actually trained here.

SEED_DATA = _build()

if __name__ == "__main__":
    import json, sys
    from collections import Counter
    json.dump(SEED_DATA, sys.stdout, indent=2)
    by_tool = Counter(s["tool"] for s in SEED_DATA)
    concepts = len({s["concept"] for s in SEED_DATA})
    print(f"\n// {len(SEED_DATA)} seeds, {concepts} concepts", file=sys.stderr)
    for tool, n in by_tool.most_common():
        print(f"//   {tool}: {n}", file=sys.stderr)
