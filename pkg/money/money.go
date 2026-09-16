package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

var (
	// ErrCurrencyMismatch é retornado quando operações envolvem moedas distintas.
	ErrCurrencyMismatch = errors.New("currency mismatch")

	// ErrInvalidCurrency é retornado quando o código da moeda não está no padrão ISO 4217 (3 letras).
	ErrInvalidCurrency = errors.New("invalid currency: must be 3 alphabetic characters (ISO 4217)")

	// ErrInvalidAmount é retornado quando a string de valor não possui um formato decimal válido.
	ErrInvalidAmount = errors.New("invalid amount format")

	// ErrExceededScale é retornado quando o valor possui mais de 2 casas decimais.
	ErrExceededScale = errors.New("scale exceeded: maximum of 2 decimal places allowed")

	// ErrOverflow é retornado quando uma operação excede o limite de int64.
	ErrOverflow = errors.New("monetary amount integer overflow")
)

// Money é um Value Object imutável que representa uma quantia financeira em unidades mínimas (centavos).
// Dinheiro nunca utiliza tipos de ponto flutuante (float32/float64).
type Money struct {
	amount   int64
	currency string
}

// New cria uma nova instância de Money a partir de unidades mínimas (ex: 2500 centavos = 25.00 BRL).
func New(amount int64, currency string) (Money, error) {
	curr, err := validateAndNormalizeCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	return Money{
		amount:   amount,
		currency: curr,
	}, nil
}

// MustNew cria uma instância de Money a partir de unidades mínimas ou entra em panic se a moeda for inválida.
// Útil para testes ou inicializações estáticas confiáveis.
func MustNew(amount int64, currency string) Money {
	m, err := New(amount, currency)
	if err != nil {
		panic(err)
	}
	return m
}

// Zero retorna uma instância com saldo zero para a moeda informada.
func Zero(currency string) (Money, error) {
	return New(0, currency)
}

// MustZero retorna uma instância com saldo zero ou entra em panic se a moeda for inválida.
func MustZero(currency string) Money {
	return MustNew(0, currency)
}

// Parse converte uma string decimal (ex: "25.00", "25", "25.5", "-10.50") em Money sem usar floats.
// Rejeita valores vazios, notação científica, escala superior a 2 casas ou caracteres não numéricos.
func Parse(amountStr string, currency string) (Money, error) {
	curr, err := validateAndNormalizeCurrency(currency)
	if err != nil {
		return Money{}, err
	}

	trimmed := strings.TrimSpace(amountStr)
	if len(trimmed) == 0 {
		return Money{}, ErrInvalidAmount
	}

	// Rejeitar notação científica e strings especiais
	if strings.ContainsAny(trimmed, "eE") ||
		strings.EqualFold(trimmed, "nan") ||
		strings.EqualFold(trimmed, "infinity") ||
		strings.EqualFold(trimmed, "+infinity") ||
		strings.EqualFold(trimmed, "-infinity") {
		return Money{}, ErrInvalidAmount
	}

	isNegative := false
	if trimmed[0] == '-' {
		isNegative = true
		trimmed = trimmed[1:]
	} else if trimmed[0] == '+' {
		trimmed = trimmed[1:]
	}

	if len(trimmed) == 0 {
		return Money{}, ErrInvalidAmount
	}

	parts := strings.Split(trimmed, ".")
	if len(parts) > 2 {
		return Money{}, ErrInvalidAmount
	}

	intPartStr := parts[0]
	if len(intPartStr) == 0 {
		return Money{}, ErrInvalidAmount
	}

	for _, ch := range intPartStr {
		if !unicode.IsDigit(ch) {
			return Money{}, ErrInvalidAmount
		}
	}

	// Parse da parte inteira com checagem de overflow
	intPart, err := strconv.ParseInt(intPartStr, 10, 64)
	if err != nil {
		return Money{}, ErrOverflow
	}

	// Checar se a multiplicação por 100 causará overflow
	if intPart > math.MaxInt64/100 {
		return Money{}, ErrOverflow
	}

	units := intPart * 100

	var fracUnits int64
	if len(parts) == 2 {
		fracPartStr := parts[1]
		if len(fracPartStr) == 0 || len(fracPartStr) > 2 {
			if len(fracPartStr) > 2 {
				return Money{}, ErrExceededScale
			}
			return Money{}, ErrInvalidAmount
		}

		for _, ch := range fracPartStr {
			if !unicode.IsDigit(ch) {
				return Money{}, ErrInvalidAmount
			}
		}

		if len(fracPartStr) == 1 {
			fracPartStr += "0"
		}

		fracVal, err := strconv.ParseInt(fracPartStr, 10, 64)
		if err != nil {
			return Money{}, ErrInvalidAmount
		}
		fracUnits = fracVal
	}

	if units > math.MaxInt64-fracUnits {
		return Money{}, ErrOverflow
	}

	totalUnits := units + fracUnits
	if isNegative {
		totalUnits = -totalUnits
	}

	return Money{
		amount:   totalUnits,
		currency: curr,
	}, nil
}

// Amount retorna a quantia em unidades mínimas (centavos).
func (m Money) Amount() int64 {
	return m.amount
}

// Currency retorna o código ISO 4217 da moeda.
func (m Money) Currency() string {
	return m.currency
}

// IsZero retorna verdadeiro se o valor for zero.
func (m Money) IsZero() bool {
	return m.amount == 0
}

// IsPositive retorna verdadeiro se o valor for estritamente maior que zero.
func (m Money) IsPositive() bool {
	return m.amount > 0
}

// IsNegative retorna verdadeiro se o valor for estritamente menor que zero.
func (m Money) IsNegative() bool {
	return m.amount < 0
}

// FormattedAmount retorna a quantia no formato decimal com 2 casas (ex: "25.00", "-10.50").
func (m Money) FormattedAmount() string {
	absAmount := m.amount
	isNeg := false
	if absAmount < 0 {
		isNeg = true
		// Tratamento especial para MinInt64 para evitar overflow na negação
		if absAmount == math.MinInt64 {
			return "-92233720368547758.08"
		}
		absAmount = -absAmount
	}

	intPart := absAmount / 100
	fracPart := absAmount % 100

	sign := ""
	if isNeg {
		sign = "-"
	}

	return fmt.Sprintf("%s%d.%02d", sign, intPart, fracPart)
}

// String formata o valor com o código da moeda (ex: "25.00 BRL").
func (m Money) String() string {
	return fmt.Sprintf("%s %s", m.FormattedAmount(), m.currency)
}

// Add soma duas quantias da mesma moeda com verificação de overflow.
func (m Money) Add(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}

	a := m.amount
	b := other.amount

	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return Money{}, ErrOverflow
	}

	return Money{
		amount:   a + b,
		currency: m.currency,
	}, nil
}

// Sub subtrai outra quantia da mesma moeda com verificação de overflow.
func (m Money) Sub(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}

	a := m.amount
	b := other.amount

	if (b < 0 && a > math.MaxInt64+b) || (b > 0 && a < math.MinInt64+b) {
		return Money{}, ErrOverflow
	}

	return Money{
		amount:   a - b,
		currency: m.currency,
	}, nil
}

// Negate inverte o sinal do valor com proteção contra overflow em math.MinInt64.
func (m Money) Negate() (Money, error) {
	if m.amount == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{
		amount:   -m.amount,
		currency: m.currency,
	}, nil
}

// Compare compara duas quantias da mesma moeda.
// Retorna -1 se m < other, 0 se m == other, 1 se m > other.
func (m Money) Compare(other Money) (int, error) {
	if m.currency != other.currency {
		return 0, ErrCurrencyMismatch
	}
	if m.amount < other.amount {
		return -1, nil
	}
	if m.amount > other.amount {
		return 1, nil
	}
	return 0, nil
}

// Equals verifica se duas quantias possuem o mesmo valor e moeda.
func (m Money) Equals(other Money) bool {
	return m.amount == other.amount && m.currency == other.currency
}

type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// MarshalJSON serializa Money no formato {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	if m.currency == "" {
		return nil, ErrInvalidCurrency
	}
	return json.Marshal(moneyJSON{
		Amount:   m.FormattedAmount(),
		Currency: m.currency,
	})
}

// UnmarshalJSON deserializa Money a partir de {"amount":"25.00","currency":"BRL"}.
func (m *Money) UnmarshalJSON(data []byte) error {
	var mj moneyJSON
	if err := json.Unmarshal(data, &mj); err != nil {
		return err
	}
	parsed, err := Parse(mj.Amount, mj.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

func validateAndNormalizeCurrency(currency string) (string, error) {
	curr := strings.TrimSpace(currency)
	if len(curr) != 3 {
		return "", ErrInvalidCurrency
	}
	for _, ch := range curr {
		if !unicode.IsLetter(ch) {
			return "", ErrInvalidCurrency
		}
	}
	return strings.ToUpper(curr), nil
}
