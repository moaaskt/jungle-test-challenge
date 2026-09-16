package money_test

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

func TestNew(t *testing.T) {
	t.Run("valid currency", func(t *testing.T) {
		m, err := money.New(2500, "BRL")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if m.Amount() != 2500 {
			t.Errorf("expected 2500, got %d", m.Amount())
		}
		if m.Currency() != "BRL" {
			t.Errorf("expected BRL, got %s", m.Currency())
		}
	})

	t.Run("lowercase currency normalized to uppercase", func(t *testing.T) {
		m, err := money.New(100, "usd")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if m.Currency() != "USD" {
			t.Errorf("expected USD, got %s", m.Currency())
		}
	})

	t.Run("invalid currency length", func(t *testing.T) {
		_, err := money.New(100, "US")
		if !errors.Is(err, money.ErrInvalidCurrency) {
			t.Errorf("expected ErrInvalidCurrency, got %v", err)
		}

		_, err = money.New(100, "USDT")
		if !errors.Is(err, money.ErrInvalidCurrency) {
			t.Errorf("expected ErrInvalidCurrency, got %v", err)
		}
	})

	t.Run("invalid currency characters", func(t *testing.T) {
		_, err := money.New(100, "123")
		if !errors.Is(err, money.ErrInvalidCurrency) {
			t.Errorf("expected ErrInvalidCurrency, got %v", err)
		}
	})
}

func TestZero(t *testing.T) {
	m, err := money.Zero("BRL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.IsZero() {
		t.Errorf("expected IsZero to be true")
	}
	if m.Amount() != 0 || m.Currency() != "BRL" {
		t.Errorf("expected 0 BRL, got %s", m)
	}

	mustM := money.MustZero("BRL")
	if !mustM.Equals(m) {
		t.Errorf("MustZero does not match Zero")
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		currency    string
		expectedAmt int64
		expectedErr error
	}{
		{name: "standard two decimals", input: "25.00", currency: "BRL", expectedAmt: 2500},
		{name: "no decimals integer", input: "25", currency: "BRL", expectedAmt: 2500},
		{name: "one decimal", input: "25.5", currency: "BRL", expectedAmt: 2550},
		{name: "zero cents", input: "0.05", currency: "BRL", expectedAmt: 5},
		{name: "zero decimal", input: "0.00", currency: "BRL", expectedAmt: 0},
		{name: "zero integer", input: "0", currency: "BRL", expectedAmt: 0},
		{name: "negative value", input: "-10.50", currency: "BRL", expectedAmt: -1050},
		{name: "negative small value", input: "-0.01", currency: "BRL", expectedAmt: -1},
		{name: "positive sign", input: "+50.25", currency: "USD", expectedAmt: 5025},
		{name: "with spaces", input: "  12.34  ", currency: "EUR", expectedAmt: 1234},
		{name: "large valid number", input: "92233720368547758.07", currency: "BRL", expectedAmt: 9223372036854775807},

		// Erros esperados
		{name: "empty string", input: "", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "whitespace only", input: "   ", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "alphabetic chars", input: "abc", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "scientific notation", input: "1e5", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "nan", input: "NaN", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "infinity", input: "Infinity", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "exceeded scale 3 decimals", input: "25.005", currency: "BRL", expectedErr: money.ErrExceededScale},
		{name: "multiple dots", input: "25.0.0", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "comma separator", input: "25,00", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "trailing dot", input: "25.", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "leading dot", input: ".50", currency: "BRL", expectedErr: money.ErrInvalidAmount},
		{name: "overflow number", input: "99999999999999999999.00", currency: "BRL", expectedErr: money.ErrOverflow},
		{name: "invalid currency", input: "25.00", currency: "BR", expectedErr: money.ErrInvalidCurrency},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := money.Parse(tt.input, tt.currency)
			if tt.expectedErr != nil {
				if !errors.Is(err, tt.expectedErr) {
					t.Fatalf("expected error %v, got %v", tt.expectedErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if m.Amount() != tt.expectedAmt {
				t.Errorf("expected amount %d, got %d", tt.expectedAmt, m.Amount())
			}
			if m.Currency() != tt.currency {
				t.Errorf("expected currency %s, got %s", tt.currency, m.Currency())
			}
		})
	}
}

func TestArithmetic(t *testing.T) {
	t.Run("Add success", func(t *testing.T) {
		m1 := money.MustNew(1000, "BRL")
		m2 := money.MustNew(1550, "BRL")

		sum, err := m1.Add(m2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sum.Amount() != 2550 {
			t.Errorf("expected 2550, got %d", sum.Amount())
		}
	})

	t.Run("Add currency mismatch", func(t *testing.T) {
		m1 := money.MustNew(1000, "BRL")
		m2 := money.MustNew(1000, "USD")

		_, err := m1.Add(m2)
		if !errors.Is(err, money.ErrCurrencyMismatch) {
			t.Errorf("expected ErrCurrencyMismatch, got %v", err)
		}
	})

	t.Run("Add overflow", func(t *testing.T) {
		m1 := money.MustNew(math.MaxInt64, "BRL")
		m2 := money.MustNew(1, "BRL")

		_, err := m1.Add(m2)
		if !errors.Is(err, money.ErrOverflow) {
			t.Errorf("expected ErrOverflow, got %v", err)
		}
	})

	t.Run("Sub success", func(t *testing.T) {
		m1 := money.MustNew(2550, "BRL")
		m2 := money.MustNew(1000, "BRL")

		diff, err := m1.Sub(m2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if diff.Amount() != 1550 {
			t.Errorf("expected 1550, got %d", diff.Amount())
		}
	})

	t.Run("Sub currency mismatch", func(t *testing.T) {
		m1 := money.MustNew(1000, "BRL")
		m2 := money.MustNew(1000, "EUR")

		_, err := m1.Sub(m2)
		if !errors.Is(err, money.ErrCurrencyMismatch) {
			t.Errorf("expected ErrCurrencyMismatch, got %v", err)
		}
	})

	t.Run("Sub overflow", func(t *testing.T) {
		m1 := money.MustNew(math.MinInt64, "BRL")
		m2 := money.MustNew(1, "BRL")

		_, err := m1.Sub(m2)
		if !errors.Is(err, money.ErrOverflow) {
			t.Errorf("expected ErrOverflow, got %v", err)
		}
	})

	t.Run("Negate", func(t *testing.T) {
		m := money.MustNew(1500, "BRL")
		neg, err := m.Negate()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if neg.Amount() != -1500 {
			t.Errorf("expected -1500, got %d", neg.Amount())
		}

		// MinInt64 overflow on negate
		mMin := money.MustNew(math.MinInt64, "BRL")
		_, err = mMin.Negate()
		if !errors.Is(err, money.ErrOverflow) {
			t.Errorf("expected ErrOverflow on negate MinInt64, got %v", err)
		}
	})
}

func TestComparison(t *testing.T) {
	m1 := money.MustNew(1000, "BRL")
	m2 := money.MustNew(2000, "BRL")
	m3 := money.MustNew(1000, "BRL")
	mUSD := money.MustNew(1000, "USD")

	cmp, err := m1.Compare(m2)
	if err != nil || cmp != -1 {
		t.Errorf("expected -1, got %d, err: %v", cmp, err)
	}

	cmp, err = m2.Compare(m1)
	if err != nil || cmp != 1 {
		t.Errorf("expected 1, got %d, err: %v", cmp, err)
	}

	cmp, err = m1.Compare(m3)
	if err != nil || cmp != 0 {
		t.Errorf("expected 0, got %d, err: %v", cmp, err)
	}

	_, err = m1.Compare(mUSD)
	if !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("expected ErrCurrencyMismatch, got %v", err)
	}

	if !m1.Equals(m3) {
		t.Errorf("expected m1 to equal m3")
	}
	if m1.Equals(m2) {
		t.Errorf("expected m1 not to equal m2")
	}
	if m1.Equals(mUSD) {
		t.Errorf("expected m1 not to equal mUSD")
	}

	if !money.MustNew(0, "BRL").IsZero() {
		t.Errorf("expected zero")
	}
	if !m1.IsPositive() {
		t.Errorf("expected positive")
	}
	if !money.MustNew(-50, "BRL").IsNegative() {
		t.Errorf("expected negative")
	}
}

func TestFormattingAndJSON(t *testing.T) {
	m := money.MustNew(2500, "BRL")
	if m.FormattedAmount() != "25.00" {
		t.Errorf("expected 25.00, got %s", m.FormattedAmount())
	}
	if m.String() != "25.00 BRL" {
		t.Errorf("expected 25.00 BRL, got %s", m.String())
	}

	mNeg := money.MustNew(-50, "BRL")
	if mNeg.FormattedAmount() != "-0.50" {
		t.Errorf("expected -0.50, got %s", mNeg.FormattedAmount())
	}

	// Test MarshalJSON
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	expectedJSON := `{"amount":"25.00","currency":"BRL"}`
	if string(data) != expectedJSON {
		t.Errorf("expected %s, got %s", expectedJSON, string(data))
	}

	// Test UnmarshalJSON
	var unmarshaled money.Money
	if err := json.Unmarshal([]byte(expectedJSON), &unmarshaled); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if !unmarshaled.Equals(m) {
		t.Errorf("expected unmarshaled %s to equal %s", unmarshaled, m)
	}

	// Test UnmarshalJSON error
	var invalidM money.Money
	if err := json.Unmarshal([]byte(`{"amount":"invalid","currency":"BRL"}`), &invalidM); err == nil {
		t.Errorf("expected error unmarshaling invalid amount, got nil")
	}
}
