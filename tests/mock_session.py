from typing import Optional, Any, List

from algoconnect import Session


sample_datagroups = ('USEquityMarketData', 'USEquityReferenceData')
sample_datasets = {
    'USEquityMarketData': ('TradeAndQuote', 'TradeOnlyMinuteBar'),
    'USEquityReferenceData': ('BasicAdjustments', 'LookupBase')
}
sample_columns = {
    'USEquityMarketData': {
        'TradeAndQuote': (
            ('EventDateTime', 'EventType', 'Ticker', 'SecId', 'Price', 'Quantity', 'Exchange', 'ConditionCode'),
            ("DateTime64(9, 'US/Eastern')", "Enum8('TRADE' = 1, 'TRADE NB' = 2, 'TRADE CANCELLED' = 3, 'TRADE NB CANCELLED' = 4, 'QUOTE ASK' = 5, 'QUOTE ASK NB' = 6, 'QUOTE BID' = 7, 'QUOTE BID NB' = 8)", 'LowCardinality(String)', 'UInt64', 'Decimal(12, 4)', 'UInt64', 'LowCardinality(String)', 'UInt32'),
            ('Event timestamp (EST) with a nanosecond resolution (milliseconds before 2016)', 'The type of the trade/quote event', 'Symbol name', 'A unique identifier for a security', 'The price of Bid, Ask, or Trade. Can be up to 4 decimal places for sub-penny prices', 'The number of shares', 'The exchange or reporting venue', 'Condition flags applicable to the trade/quote encoded as unsigned int')
        ),
        'TradeOnlyMinuteBar': (
            ('BarDateTime', 'Ticker', 'SecId', 'FirstTradePrice', 'HighTradePrice', 'LowTradePrice', 'LastTradePrice', 'VolumeWeightPrice', 'Volume', 'TotalTrades'),
            ("DateTime('US/Eastern')", 'LowCardinality(String)', 'UInt64', 'Decimal(12, 4)', 'Decimal(12, 4)', 'Decimal(12, 4)', 'Decimal(12, 4)', 'Decimal(18, 6)', 'UInt32', 'UInt32'),
            ('The timestamp of the bar start (EST)', 'Symbol name', 'A unique identifier for a security', 'Price of the first trade', 'Trade with the highest price', 'Trade with the lowest price', 'Price of the last trade', 'Volume-weighted average price', 'Total number of shares traded', 'Total number of trades')

        ),
    },
    'USEquityReferenceData': {
        'BasicAdjustments': (
            ('SecId', 'Ticker', 'EffectiveDate', 'AdjustmentFactor', 'AdjustmentReason', 'EventId'),
            ('UInt64', 'LowCardinality(String)', 'Date', 'Float64', 'LowCardinality(String)', 'UInt64'),
            ('A unique identifier for a security', 'Symbol name', 'The date on which the event becomes effective', 'The value of the Adjustment factor for the event', 'The reason for the Corporate Event', 'Unique Event ID under the AdjustmentReason')
        ),
        'LookupBase': (
            ('SecId', 'Ticker', 'StartDate', 'EndDate'),
            ('UInt64', 'LowCardinality(String)', 'Date', 'Date'),
            ('A unique identifier for a security', 'Symbol name', 'The start date', 'The end date')
        ),
    }
}


class MockSession(Session):

    def __init__(
            self,
            host: Optional[str] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            secure: bool = False,
            **kwargs: Any
            ):
        self._host = host
        self._user = user
        self._password = password
        self._secure = secure

        self.client = "Client"

    def close(self):
        pass

    def execute(self, query: str, **kwargs: Any) -> List[tuple]:
        if query == 'SEELCT 1':
            return [(1, )]
        elif query == 'SHOW DATABASES':
            return [sample_datagroups]
        elif query.startswith('SHOW TABLES FROM '):
            datagroup = query.rsplit(' ', 1)[-1]
            if datagroup in sample_datasets:
                return [sample_datasets[datagroup]]
        elif query.startswith('DESCRIBE TABLE '):
            datagroup, dataset = query.split(' ')[-1].split('.', 1)
            payload = sample_columns.get(datagroup, {}).get(dataset)
            if payload:
                return payload[0], payload[1], [], [], payload[2], [], []
        return []
