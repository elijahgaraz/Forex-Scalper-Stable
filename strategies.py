from abc import ABC, abstractmethod
from typing import Any, List, Sequence, Dict
from statistics import mean


def _sma(values: Sequence[float], window: int) -> float:
    return mean(values) if len(values) < window else mean(values[-window:])


class Strategy(ABC):
    """Abstract base class for trading strategies."""

    @abstractmethod
    def decide(self, market_data: Dict[str, Any]) -> str:
        """Return one of: 'buy', 'sell', or 'hold'."""
        ...


class SafeStrategy(Strategy):
    """Conservative strategy using long and very long SMAs."""

    def __init__(self, short_window: int = 20, long_window: int = 50):
        self.short_window = short_window
        self.long_window = long_window

    def decide(self, market_data: Dict[str, Any]) -> str:
        prices: List[float] = market_data.get('prices', [])
        if len(prices) < 2:
            return 'hold'
        short = _sma(prices, self.short_window)
        long = _sma(prices, self.long_window)
        if short > long:
            return 'buy'
        elif short < long:
            return 'sell'
        else:
            return 'hold'


class ModerateStrategy(Strategy):
    """Balanced strategy with mid-range SMAs."""

    def __init__(self, short_window: int = 10, long_window: int = 30):
        self.short_window = short_window
        self.long_window = long_window

    def decide(self, market_data: Dict[str, Any]) -> str:
        prices: List[float] = market_data.get('prices', [])
        if len(prices) < 2:
            return 'hold'
        short = _sma(prices, self.short_window)
        long = _sma(prices, self.long_window)
        if short > long:
            return 'buy'
        elif short < long:
            return 'sell'
        else:
            return 'hold'


class AggressiveStrategy(Strategy):
    """Fast-reacting strategy using short SMAs."""

    def __init__(self, short_window: int = 5, long_window: int = 15):
        self.short_window = short_window
        self.long_window = long_window

    def decide(self, market_data: Dict[str, Any]) -> str:
        prices: List[float] = market_data.get('prices', [])
        if len(prices) < 2:
            return 'hold'
        short = _sma(prices, self.short_window)
        long = _sma(prices, self.long_window)
        if short > long:
            return 'buy'
        elif short < long:
            return 'sell'
        else:
            return 'hold'


class MomentumStrategy(Strategy):
    """Buys when momentum is positive, sells when negative."""

    def __init__(self, window: int = 14):
        self.window = window

    def decide(self, market_data: Dict[str, Any]) -> str:
        prices: List[float] = market_data.get('prices', [])
        if len(prices) < self.window + 1:
            return 'hold'
        momentum = prices[-1] - prices[-self.window - 1]
        if momentum > 0:
            return 'buy'
        elif momentum < 0:
            return 'sell'
        else:
            return 'hold'


class MeanReversionStrategy(Strategy):
    """Reverts when price is far from its SMA."""

    def __init__(self, window: int = 20, threshold: float = 0.002):
        self.window = window
        self.threshold = threshold

    def decide(self, market_data: Dict[str, Any]) -> str:
        prices: List[float] = market_data.get('prices', [])
        if len(prices) < self.window + 1:
            return 'hold'
        sma = _sma(prices, self.window)
        current = prices[-1]
        deviation = (current - sma) / sma
        if deviation > self.threshold:
            return 'sell'
        elif deviation < -self.threshold:
            return 'buy'
        else:
            return 'hold'
