"""Минимальные тесты бизнес-формул (stdlib: python -m unittest discover)."""
from decimal import Decimal
import unittest


def order_reward(amount: str, monthly_rating: str) -> Decimal:
    amount, r = Decimal(amount), Decimal(monthly_rating)
    if r < 4:
        pct, minimum = Decimal('0.05'), Decimal(100)
    elif r < Decimal('4.5'):
        pct, minimum = Decimal('0.07'), Decimal(150)
    elif r < Decimal('4.9'):
        pct, minimum = Decimal('0.08'), Decimal(175)
    else:
        pct, minimum = Decimal('0.10'), Decimal(200)
    return max(amount * pct, minimum)


class TestCourierLedger(unittest.TestCase):
    def test_tiers_and_minimums(self):
        self.assertEqual(order_reward('1000', '3.99'), 100)
        self.assertEqual(order_reward('5000', '3.99'), 250)
        self.assertEqual(order_reward('1000', '4.00'), 150)
        self.assertEqual(order_reward('5000', '4.49'), 350)
        self.assertEqual(order_reward('1000', '4.50'), 175)
        self.assertEqual(order_reward('5000', '4.89'), 400)
        self.assertEqual(order_reward('1000', '4.90'), 200)
        self.assertEqual(order_reward('5000', '4.90'), 500)

    def test_monthly_rating_must_not_be_rounded_before_bracket(self):
        self.assertEqual(order_reward('5000', '4.899999'), 400)
        self.assertEqual(order_reward('5000', '4.900000'), 500)

    def test_two_order_month(self):
        # Средняя оценка = (5+4)/2 = 4.5: 8% с минимумом 175.
        rewards = order_reward('1000', '4.5') + order_reward('3000', '4.5')
        tips = Decimal('100') + Decimal('200')
        self.assertEqual(rewards, 415)
        self.assertEqual(rewards + tips * Decimal('0.95'), Decimal('700.00'))
        self.assertEqual((Decimal('1000') + Decimal('3000')) * Decimal('0.25'), 1000)


if __name__ == '__main__':
    unittest.main()
