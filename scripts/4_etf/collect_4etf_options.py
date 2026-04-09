#!/usr/bin/env python3
"""
Options Data Collector for SOXL PMCC Strategy
Collects all relevant Greeks, IV, and pricing data for both long LEAPS and short calls
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import sys
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURATION
# ============================================================

# Long leg (LEAPS) configuration
LEAPS_EXPIRATION = "2027-01-15"  # Target LEAPS expiration (adjust as needed)
LEAPS_MIN_DELTA = 0.70  # Minimum delta for deep ITM call
LEAPS_MAX_STRIKE_PCT = 0.90  # Strike at or below 90% of current price

# Short leg configuration
SHORT_STRIKE_PCT = 1.10  # Sell calls 10-15% above current price
SHORT_DAYS_TO_EXPIRATION = 7  # Weekly calls (7 days)
SHORT_MIN_PREMIUM = 0.50  # Minimum premium to consider selling ($50 per contract)

# File to save options data
OPTIONS_DATA_PATH = "data/4_etf/options_data.csv"
OPTIONS_SUMMARY_PATH = "data/4_etf/options_summary.csv"


def get_nearest_expiration(expirations, target_days=7):
    """Get the expiration date closest to target days from now"""
    today = datetime.now().date()
    target_date = today + timedelta(days=target_days)
    
    exp_dates = []
    for exp in expirations:
        try:
            exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
            if exp_date > today:
                exp_dates.append((exp_date, exp))
        except:
            continue
    
    if not exp_dates:
        return None
    
    closest = min(exp_dates, key=lambda x: abs((x[0] - target_date).days))
    return closest[1]


def calculate_option_metrics(option_row, current_price):
    """Calculate additional metrics for an option"""
    mid_price = (option_row["bid"] + option_row["ask"]) / 2 if option_row["bid"] > 0 and option_row["ask"] > 0 else None
    
    # Calculate intrinsic value (for calls: max(0, price - strike))
    intrinsic = max(0, current_price - option_row["strike"]) if current_price else 0
    
    # Extrinsic value (time value) = price - intrinsic
    extrinsic = mid_price - intrinsic if mid_price and intrinsic is not None else None
    
    # Percent of strike (premium as % of strike price)
    premium_pct = (mid_price / option_row["strike"] * 100) if mid_price and option_row["strike"] > 0 else None
    
    return {
        "mid": round(mid_price, 2) if mid_price else None,
        "intrinsic": round(intrinsic, 2),
        "extrinsic": round(extrinsic, 2) if extrinsic else None,
        "premium_pct": round(premium_pct, 2) if premium_pct else None,
    }


def find_best_leaps(expirations, soxl, current_price):
    """
    Find the best LEAPS contract for PMCC based on:
    - Deep ITM (delta >= 0.70)
    - Reasonable cost
    - Good liquidity (open interest)
    """
    best_leaps = None
    best_score = -1
    
    for exp in expirations:
        # Only look at far-dated expirations (6+ months out)
        exp_date = datetime.strptime(exp, "%Y-%m-%d")
        months_out = (exp_date.year - datetime.now().year) * 12 + (exp_date.month - datetime.now().month)
        if months_out < 6:
            continue
        
        try:
            chain = soxl.option_chain(exp)
            calls = chain.calls
            
            # Filter for deep ITM calls (strike <= current_price * 0.90)
            itm_calls = calls[calls["strike"] <= current_price * LEAPS_MAX_STRIKE_PCT]
            
            for _, row in itm_calls.iterrows():
                delta = row["delta"] if row["delta"] else 0
                if delta < LEAPS_MIN_DELTA:
                    continue
                
                # Calculate metrics
                metrics = calculate_option_metrics(row, current_price)
                mid_price = metrics["mid"]
                if not mid_price:
                    continue
                
                # Score: higher delta is better, lower cost is better, higher OI is better
                delta_score = delta * 100
                cost_score = max(0, 100 - (mid_price / current_price * 100))
                liquidity_score = min(50, row["openInterest"] / 100) if row["openInterest"] else 0
                
                total_score = delta_score + cost_score + liquidity_score
                
                if total_score > best_score:
                    best_score = total_score
                    best_leaps = {
                        "expiration": exp,
                        "strike": row["strike"],
                        "bid": row["bid"],
                        "ask": row["ask"],
                        "mid": mid_price,
                        "delta": delta,
                        "gamma": row["gamma"] if row["gamma"] else 0,
                        "theta": row["theta"] if row["theta"] else 0,
                        "vega": row["vega"] if row["vega"] else 0,
                        "rho": row["rho"] if row["rho"] else 0,
                        "implied_volatility": row["impliedVolatility"] * 100 if row["impliedVolatility"] else 0,
                        "open_interest": row["openInterest"] if row["openInterest"] else 0,
                        "volume": row["volume"] if row["volume"] else 0,
                        "intrinsic": metrics["intrinsic"],
                        "extrinsic": metrics["extrinsic"],
                        "premium_pct": metrics["premium_pct"],
                        "months_out": months_out,
                    }
                    
        except Exception as e:
            continue
    
    return best_leaps


def collect_options_data():
    """Collect comprehensive options data for SOXL PMCC"""
    
    print("=" * 70)
    print("SOXL PMCC OPTIONS DATA COLLECTOR")
    print("=" * 70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize ticker
    soxl = yf.Ticker("SOXL")
    
    # Get current stock price
    hist = soxl.history(period="5d")
    if hist.empty:
        print("ERROR: Could not fetch SOXL price data")
        return None
    
    current_price = hist["Close"].iloc[-1]
    prev_close = hist["Close"].iloc[-2] if len(hist) > 1 else current_price
    daily_change = ((current_price - prev_close) / prev_close) * 100
    
    # Get 20-day average volume
    avg_volume_20 = hist["Volume"].tail(20).mean() if len(hist) >= 20 else hist["Volume"].mean()
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n📊 SOXL Current Price: ${current_price:.2f} ({daily_change:+.2f}%)")
    print(f"   20-Day Avg Volume: {avg_volume_20:.0f}")
    
    # Get available expiration dates
    expirations = soxl.options
    if not expirations:
        print("ERROR: No option expiration dates available")
        return None
    
    print(f"   Available expirations: {len(expirations)} dates")
    
    # ============================================================
    # 1. Find Best LEAPS Contract (Long Leg)
    # ============================================================
    print("\n" + "-" * 50)
    print("🔍 SEARCHING FOR BEST LEAPS CONTRACT")
    print("-" * 50)
    
    best_leaps = find_best_leaps(expirations, soxl, current_price)
    
    leaps_data = {
        "expiration": None,
        "months_out": None,
        "strike": None,
        "bid": None,
        "ask": None,
        "mid": None,
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "rho": None,
        "implied_volatility": None,
        "open_interest": None,
        "volume": None,
        "intrinsic": None,
        "extrinsic": None,
        "premium_pct": None,
        "available": False,
    }
    
    if best_leaps:
        leaps_data = {
            "expiration": best_leaps["expiration"],
            "months_out": best_leaps["months_out"],
            "strike": best_leaps["strike"],
            "bid": best_leaps["bid"],
            "ask": best_leaps["ask"],
            "mid": best_leaps["mid"],
            "delta": best_leaps["delta"],
            "gamma": best_leaps["gamma"],
            "theta": best_leaps["theta"],
            "vega": best_leaps["vega"],
            "rho": best_leaps["rho"],
            "implied_volatility": best_leaps["implied_volatility"],
            "open_interest": best_leaps["open_interest"],
            "volume": best_leaps["volume"],
            "intrinsic": best_leaps["intrinsic"],
            "extrinsic": best_leaps["extrinsic"],
            "premium_pct": best_leaps["premium_pct"],
            "available": True,
        }
        
        print(f"\n✅ Best LEAPS Found:")
        print(f"   Expiration: {best_leaps['expiration']} ({best_leaps['months_out']} months)")
        print(f"   Strike: ${best_leaps['strike']:.2f}")
        print(f"   Mid Price: ${best_leaps['mid']:.2f}")
        print(f"   Delta: {best_leaps['delta']:.3f} | Gamma: {best_leaps['gamma']:.4f}")
        print(f"   Theta: {best_leaps['theta']:.4f} | Vega: {best_leaps['vega']:.4f}")
        print(f"   IV: {best_leaps['implied_volatility']:.1f}%")
        print(f"   Intrinsic: ${best_leaps['intrinsic']:.2f} | Extrinsic: ${best_leaps['extrinsic']:.2f}")
        print(f"   Open Interest: {best_leaps['open_interest']:,}")
    else:
        print("\n⚠️ No suitable LEAPS contract found")
    
    # ============================================================
    # 2. Get Short-Term Call (Short Leg)
    # ============================================================
    print("\n" + "-" * 50)
    print("📝 SHORT-TERM CALL (Weekly Premium)")
    print("-" * 50)
    
    short_data = {
        "expiration": None,
        "days_to_exp": None,
        "strike": None,
        "bid": None,
        "ask": None,
        "mid": None,
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "implied_volatility": None,
        "open_interest": None,
        "volume": None,
        "premium_pct": None,
        "available": False,
    }
    
    target_strike = round(current_price * SHORT_STRIKE_PCT, 1)
    nearest_exp = get_nearest_expiration(expirations, SHORT_DAYS_TO_EXPIRATION)
    
    if nearest_exp:
        try:
            chain = soxl.option_chain(nearest_exp)
            calls = chain.calls
            
            # Find OTM calls at or above target strike
            otm_calls = calls[calls["strike"] >= target_strike]
            
            if not otm_calls.empty:
                short_call = otm_calls.iloc[0]
                metrics = calculate_option_metrics(short_call, current_price)
                
                exp_date = datetime.strptime(nearest_exp, "%Y-%m-%d")
                days = (exp_date.date() - datetime.now().date()).days
                
                short_data = {
                    "expiration": nearest_exp,
                    "days_to_exp": days,
                    "strike": short_call["strike"],
                    "bid": short_call["bid"],
                    "ask": short_call["ask"],
                    "mid": metrics["mid"],
                    "delta": short_call["delta"] if short_call["delta"] else 0,
                    "gamma": short_call["gamma"] if short_call["gamma"] else 0,
                    "theta": short_call["theta"] if short_call["theta"] else 0,
                    "vega": short_call["vega"] if short_call["vega"] else 0,
                    "implied_volatility": short_call["impliedVolatility"] * 100 if short_call["impliedVolatility"] else 0,
                    "open_interest": short_call["openInterest"] if short_call["openInterest"] else 0,
                    "volume": short_call["volume"] if short_call["volume"] else 0,
                    "premium_pct": metrics["premium_pct"],
                    "available": True,
                }
                
                print(f"\n✅ Short Call Found:")
                print(f"   Expiration: {nearest_exp} ({days} days)")
                print(f"   Strike: ${short_data['strike']:.2f}")
                print(f"   Mid Price: ${short_data['mid']:.2f}")
                print(f"   Premium as % of strike: {short_data['premium_pct']:.2f}%")
                print(f"   Delta: {short_data['delta']:.3f} | Theta: {short_data['theta']:.4f}")
                print(f"   IV: {short_data['implied_volatility']:.1f}%")
                print(f"   Open Interest: {short_data['open_interest']:,}")
                
                # Check if premium meets minimum threshold
                if short_data['mid'] and short_data['mid'] >= SHORT_MIN_PREMIUM:
                    print(f"   ✅ Premium meets minimum threshold (${SHORT_MIN_PREMIUM:.2f})")
                else:
                    print(f"   ⚠️ Premium below minimum threshold (${SHORT_MIN_PREMIUM:.2f})")
            else:
                print(f"\n⚠️ No OTM calls found at or above ${target_strike:.1f}")
                
        except Exception as e:
            print(f"Error fetching short-term options data: {e}")
    else:
        print(f"\n⚠️ Could not find expiration near {SHORT_DAYS_TO_EXPIRATION} days")
    
    # ============================================================
    # 3. Get Put Option Data (For Hedging/Bearish)
    # ============================================================
    print("\n" + "-" * 50)
    print("🛡️ PUT REFERENCE (Hedging Context)")
    print("-" * 50)
    
    put_data = {
        "strike": None,
        "bid": None,
        "ask": None,
        "mid": None,
        "delta": None,
        "implied_volatility": None,
        "available": False,
    }
    
    if len(expirations) > 0:
        try:
            chain = soxl.option_chain(expirations[0])
            puts = chain.puts
            
            # Find OTM puts ~10% below current price
            target_put_strike = round(current_price * 0.90, 1)
            otm_puts = puts[puts["strike"] <= target_put_strike]
            
            if not otm_puts.empty:
                put = otm_puts.iloc[-1]
                metrics = calculate_option_metrics(put, current_price)
                
                put_data = {
                    "strike": put["strike"],
                    "bid": put["bid"],
                    "ask": put["ask"],
                    "mid": metrics["mid"],
                    "delta": put["delta"] if put["delta"] else 0,
                    "implied_volatility": put["impliedVolatility"] * 100 if put["impliedVolatility"] else 0,
                    "available": True,
                }
                
                print(f"\n✅ Put Reference Found:")
                print(f"   Expiration: {expirations[0]}")
                print(f"   Strike: ${put_data['strike']:.2f}")
                print(f"   Mid Price: ${put_data['mid']:.2f}")
                print(f"   Delta: {put_data['delta']:.3f}")
                print(f"   IV: {put_data['implied_volatility']:.1f}%")
                
        except Exception as e:
            print(f"Error fetching put data: {e}")
    
    # ============================================================
    # 4. Calculate VIX (Market Volatility Context)
    # ============================================================
    try:
        vix = yf.Ticker("^VIX")
        vix_hist = vix.history(period="5d")
        vix_current = vix_hist["Close"].iloc[-1] if not vix_hist.empty else None
        vix_change = ((vix_hist["Close"].iloc[-1] - vix_hist["Close"].iloc[-2]) / vix_hist["Close"].iloc[-2] * 100) if len(vix_hist) > 1 else None
    except:
        vix_current = None
        vix_change = None
    
    # ============================================================
    # 5. Save All Data to CSV
    # ============================================================
    
    # Build the data row
    data_row = {
        "date": current_date,
        "soxl_price": round(current_price, 2),
        "soxl_daily_pct": round(daily_change, 2),
        "soxl_avg_volume_20": round(avg_volume_20, 0),
        "vix": round(vix_current, 2) if vix_current else None,
        "vix_daily_pct": round(vix_change, 2) if vix_change else None,
        
        # LEAPS data
        "leaps_expiration": leaps_data["expiration"],
        "leaps_months_out": leaps_data["months_out"],
        "leaps_strike": leaps_data["strike"],
        "leaps_bid": leaps_data["bid"],
        "leaps_ask": leaps_data["ask"],
        "leaps_mid": leaps_data["mid"],
        "leaps_delta": leaps_data["delta"],
        "leaps_gamma": leaps_data["gamma"],
        "leaps_theta": leaps_data["theta"],
        "leaps_vega": leaps_data["vega"],
        "leaps_rho": leaps_data["rho"],
        "leaps_iv": leaps_data["implied_volatility"],
        "leaps_oi": leaps_data["open_interest"],
        "leaps_volume": leaps_data["volume"],
        "leaps_intrinsic": leaps_data["intrinsic"],
        "leaps_extrinsic": leaps_data["extrinsic"],
        "leaps_premium_pct": leaps_data["premium_pct"],
        "leaps_available": leaps_data["available"],
        
        # Short call data
        "short_expiration": short_data["expiration"],
        "short_days": short_data["days_to_exp"],
        "short_strike": short_data["strike"],
        "short_bid": short_data["bid"],
        "short_ask": short_data["ask"],
        "short_mid": short_data["mid"],
        "short_delta": short_data["delta"],
        "short_gamma": short_data["gamma"],
        "short_theta": short_data["theta"],
        "short_vega": short_data["vega"],
        "short_iv": short_data["implied_volatility"],
        "short_oi": short_data["open_interest"],
        "short_volume": short_data["volume"],
        "short_premium_pct": short_data["premium_pct"],
        "short_available": short_data["available"],
        
        # Put reference data
        "put_strike": put_data["strike"],
        "put_bid": put_data["bid"],
        "put_ask": put_data["ask"],
        "put_mid": put_data["mid"],
        "put_delta": put_data["delta"],
        "put_iv": put_data["implied_volatility"],
        "put_available": put_data["available"],
    }
    
    # Create or append to CSV
    df_new = pd.DataFrame([data_row])
    
    try:
        existing = pd.read_csv(OPTIONS_DATA_PATH)
        # Remove any existing row for today (avoid duplicates if run multiple times)
        existing = existing[existing["date"] != current_date]
        df_combined = pd.concat([existing, df_new], ignore_index=True)
        df_combined.to_csv(OPTIONS_DATA_PATH, index=False)
        print(f"\n✅ Appended data to {OPTIONS_DATA_PATH}")
        print(f"   Total rows: {len(df_combined)}")
    except FileNotFoundError:
        df_new.to_csv(OPTIONS_DATA_PATH, index=False)
        print(f"\n✅ Created new file: {OPTIONS_DATA_PATH}")
    
    # ============================================================
    # 6. Generate Summary Statistics
    # ============================================================
    print("\n" + "=" * 70)
    print("PMCC READINESS SUMMARY")
    print("=" * 70)
    
    if leaps_data["available"] and short_data["available"]:
        leaps_cost = leaps_data["mid"]
        weekly_premium = short_data["mid"]
        weeks_to_breakeven = (leaps_cost / weekly_premium) if weekly_premium and weekly_premium > 0 else None
        
        print(f"\n💰 PMCC Cost Structure:")
        print(f"   LEAPS Cost: ${leaps_cost:.2f}")
        print(f"   Weekly Premium: ${weekly_premium:.2f}")
        if weeks_to_breakeven:
            print(f"   Weeks to Recoup LEAPS Cost: {weeks_to_breakeven:.1f} weeks")
        
        print(f"\n📈 Risk Metrics:")
        print(f"   LEAPS Delta: {leaps_data['delta']:.3f} (exposure to SOXL)")
        print(f"   LEAPS Vega: {leaps_data['vega']:.4f} (sensitivity to IV)")
        print(f"   LEAPS Theta: {leaps_data['theta']:.4f} (daily decay)")
        print(f"   Short Call Theta: +{abs(short_data['theta']):.4f} (daily premium decay)")
        print(f"   Net Theta: {leaps_data['theta'] + abs(short_data['theta']):.4f}")
        
        print(f"\n📊 Current IV Environment:")
        print(f"   LEAPS IV: {leaps_data['implied_volatility']:.1f}%")
        print(f"   Short Call IV: {short_data['implied_volatility']:.1f}%")
        if vix_current:
            print(f"   VIX: {vix_current:.2f}")
    else:
        print("\n⚠️ Cannot calculate PMCC readiness - missing LEAPS or short call data")
    
    print("\n" + "=" * 70)
    print("OPTIONS DATA COLLECTION COMPLETE")
    print("=" * 70)
    
    return data_row


def main():
    try:
        collect_options_data()
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
