package reduce; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.StringReader;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import writable.CompositeKey;
import writable.DayStatsWritable;

/*********************************************************************
 * *******************************************************************
 * author: 	Daniel Sullivan
 * date:	May 5, 2015
 * overveiw:	
 * 			Simple hybrib of rule sets 1 and 2
 * 			-Only enter positions when in an "uptrend" when short EMA > long EMA
 * 			-Use new Hi's and Lows once an uptrend is established. 
 *
 *********************************************************************
 *********************************************************************/

public class HybridReducer extends Reducer<CompositeKey,DayStatsWritable,Text,Text>{

	private String date;
	private String ticker; 
	private Text newKey = new Text(); 
	private Text valout = new Text(); 

	private String prevTicker = ""; 

	private final double startCapitalDefault = 5000.00; 

	private final static int pricesSize = 4;
	private final static int open = 0; 
	private final static int high = 1; 
	private final static int low = 2; 
	private final static int close = 3;
	private double[] prices = new double[pricesSize]; 

	private final int nSize = 3;
	private final int halfN = 0; 
	private final int oneN = 1; 
	private final int twoN = 2; 
	private double[] nVals = new double[nSize];


	private final int emaSize = 6; 
	private final int ema12 = 0; 
	private final int ema26 = 1;
	private final int ema50 = 2; 
	private final int ema75 = 3;
	private final int ema150 = 4; 
	private final int ema200 = 5; 
	private double[] thisEMA = new double[emaSize]; 

	private final static int numHiLows = 7; 
	private final static int twentyHigh = 0; 
	private final static int thirtyHigh = 1; 
	private final static int fittyFiveHigh = 2; 
	private final static int twoFittyHigh = 3; 
	private final static int tenLow = 4; 
	private final static int twentyLow = 5; 
	private final static int thirtyLow = 6; 
	private double[] daysHiLows = new double[numHiLows]; 


	private final int posSize = 18; 
	private final int pos12_75_20h_10l = 0;
	private final int pos12_75_30h_10l = 1;
	private final int pos12_75_55h_10l = 2;
	private final int pos12_75_20h_20l = 3;
	private final int pos12_75_30h_20l = 4;
	private final int pos12_75_55h_20l = 5;
	private final int pos26_50_20h_10l = 6; 
	private final int pos26_50_30h_10l = 7; 
	private final int pos26_50_55h_10l = 8; 
	private final int pos26_50_20h_20l = 9; 
	private final int pos26_50_30h_20l = 10; 
	private final int pos26_50_55h_20l = 11; 
	private final int pos50_150_20h_10l = 12; 
	private final int pos50_150_30h_10l = 13;
	private final int pos50_150_55h_10l = 14;
	private final int pos50_150_20h_20l = 15; 
	private final int pos50_150_30h_20l = 16;
	private final int pos50_150_55h_20l = 17;
	private boolean[] posEntered = new boolean[posSize*3];
	private boolean[] uptrend = new boolean[posSize*3]; 
	private double[] entryPrice = new double[posSize*3]; 
	private double[] exitPrice = new double[posSize*3]; 
	private int[] shares = new int[posSize*3];
	private double[] stopPrice = new double[posSize*3]; 
	private double[] startCapital = new double[posSize*3]; 
	private double[] entryCapital = new double[posSize*3]; 
	private double[] exitCapital = new double[posSize*3];
	private double[] realizedGains = new double[posSize*3]; 
	private String[] lineBuilder = new String[posSize*3]; 

	private HashMap<String, ArrayList<String>> results = new HashMap<String, ArrayList<String>>(); 

	public void reduce(CompositeKey key, Iterable<DayStatsWritable> values, Context context) throws IOException, InterruptedException{

		//		// Testing for proper sorting...
		//		for(DayStatsWritable val: values){
		//			context.write(key.getTicker(), new Text(val.toString()));
		//		}

		ticker = key.getTicker().toString().trim(); 


		if(!key.getTicker().toString().trim().equals(prevTicker)){
			for(String r: results.keySet()){
				int posIndex = -1;
				int exitLow = -1;
//				try{
					posIndex = Integer.parseInt(r.substring(7, r.indexOf(":")).trim());
					exitLow = Integer.parseInt((r.charAt(3)+"").trim());
					if(!lineBuilder[posIndex].equals("")){
						exitPosition(posIndex, exitLow); 
					}
					for(String l: results.get(r)){
						context.write(new Text(r), new Text(l));
					}
//				} catch(Exception e) {
//					System.err.println("\n\n**********************************************");
//					System.err.println("NullPointerException on parsing stock data");
//					System.err.println("Line position length: " + lineBuilder.length);
//					System.err.println("Enter position index: " + posIndex);
//					System.err.println("Exit position index: " + exitLow);
//					e.printStackTrace();
//					System.err.println("**********************************************\n\n");
//					// System.exit(0);
//				}

			}

			results.clear();

			for(int i = 0; i < posSize * 3; i++){
				posEntered[i] = false;
				entryPrice[i] = 0.0;
				exitPrice[i] = 0.0; 
				shares[i] = 0; 
				stopPrice[i] = 0.0; 
				startCapital[i] = startCapitalDefault;
				realizedGains[i] = 0.0; 
				entryCapital[i] = 0.0;
				exitCapital[i] = 0.0; 
			}

			for(int i = 0; i < emaSize; i++) thisEMA[i] = 0.0;
			for(int i = 0; i < numHiLows; i++) daysHiLows[i] = 0.0; 
			for(int i = 0; i < nSize; i++) nVals[i] = 0.0; 
			for(int i = 0; i < pricesSize; i++) prices[i] = 0.0; 
		}

		for(DayStatsWritable val: values){

			date = val.getDate().toString().trim(); 

			prices[open] = val.getOpen().get(); 
			prices[high] = val.getHigh().get(); 
			prices[low] = val.getLow().get(); 
			prices[close] = val.getClose().get();

			nVals[halfN] = (val.getnValue().get() / 2); 
			nVals[oneN] = (val.getnValue().get()); 
			nVals[twoN] = (val.getnValue().get() * 2); 

			thisEMA[ema12] = val.getEMA12().get(); 
			thisEMA[ema26] = val.getEMA26().get(); 
			thisEMA[ema50] = val.getEMA50().get();
			thisEMA[ema75] = val.getEMA75().get(); 
			thisEMA[ema150] = val.getEMA150().get(); 
			thisEMA[ema200] = val.getEMA200().get();

			daysHiLows[twentyHigh] = val.getTwentyHigh().get(); 
			daysHiLows[thirtyHigh] = val.getThirtyHigh().get(); 
			daysHiLows[fittyFiveHigh] = val.getFiftyFiveHigh().get();
			daysHiLows[twoFittyHigh] = val.getTwoFiftyHigh().get(); 
			daysHiLows[tenLow] = val.getTenLow().get(); 
			daysHiLows[twentyLow] = val.getTwentyLow().get();
			daysHiLows[thirtyLow] = val.getThirtyLow().get();

			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema12, ema75, twentyHigh, tenLow, i, pos12_75_20h_10l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema12, ema75, thirtyHigh, tenLow, i, pos12_75_30h_10l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema12, ema75, fittyFiveHigh, tenLow, i, pos12_75_55h_10l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema12, ema75, twentyHigh, twentyLow, i, pos12_75_20h_20l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema12, ema75, thirtyHigh, twentyLow, i, pos12_75_30h_20l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema12, ema75, fittyFiveHigh, twentyLow, i, pos12_75_55h_20l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema26, ema50, twentyHigh, tenLow, i, pos12_75_20h_10l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema26, ema50, thirtyHigh, tenLow, i, pos12_75_30h_10l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema26, ema50, fittyFiveHigh, tenLow, i, pos12_75_55h_10l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema26, ema50, twentyHigh, twentyLow, i, pos12_75_20h_20l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema26, ema50, thirtyHigh, twentyLow, i, pos12_75_30h_20l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema26, ema50, fittyFiveHigh, twentyLow, i, pos12_75_55h_20l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema50, ema150, twentyHigh, tenLow, i, pos12_75_20h_10l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema50, ema150, thirtyHigh, tenLow, i, pos12_75_30h_10l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema50, ema150, fittyFiveHigh, tenLow, i, pos12_75_55h_10l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema50, ema150, twentyHigh, twentyLow, i, pos12_75_20h_20l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema50, ema150, thirtyHigh, twentyLow, i, pos12_75_30h_20l);
			}
			for(int i = 0; i < nSize; i++){
				analyzeTradeDate(ema50, ema150, fittyFiveHigh, twentyLow, i, pos12_75_55h_20l);
			}
		}		


		prevTicker = key.getTicker().toString().trim(); 

	}

	public void analyzeTradeDate(int shortEMA, int longEMA, int entryHi, int exitLow, int nVal, int posIndex){

		int trueIndex = (posIndex * 3) + nVal;  

		double thisHi = daysHiLows[entryHi];
		double thisLow = daysHiLows[exitLow];
		double shortEMAvalue = thisEMA[shortEMA]; 
		double longEMAvalue = thisEMA[longEMA]; 
		double thisN = nVals[nVal]; 

		// IF ANY OF THESE CONDITIONS EXIST
		// 		Not enough data to run yet so just return 
		if(Math.abs(longEMAvalue) == 99999.99) return; 
		if(Math.abs(shortEMAvalue) == 99999.99) return; 
		if(Math.abs(thisHi) == 99999.99) return;
		if(Math.abs(thisLow) == 99999.99) return; 
		if(Math.abs(thisN) == 99999.99) return;

		newKey.set("HY"+shortEMA+""+longEMA+""+entryHi+""+exitLow+""+nVal+""+trueIndex+":"+ticker);
		if(!results.containsKey(newKey.toString())){
			ArrayList<String> a = new ArrayList<String>(); 
			results.put(newKey.toString(), a); 
		}

		/****************************************************************
		 * 
		 * if not uptrend 
		 * 		check for uptrend conditions and change if necessary
		 * 		check if position entered and check for exit conditions
		 *  
		 * 
		 * 
		 ***************************************************************/

		if(!uptrend[trueIndex]){
			if(exitConditionsMet(trueIndex, exitLow)) exitPosition(trueIndex, exitLow); 
			uptrend[trueIndex] = shortEMAvalue > longEMAvalue; 
			return;
		}

		if(uptrend[trueIndex] && posEntered[trueIndex]){
			uptrend[trueIndex] = shortEMAvalue > longEMAvalue; 
			if(exitConditionsMet(trueIndex, exitLow)) exitPosition(trueIndex, exitLow);
			return; 
		}
		if(uptrend[trueIndex] && !posEntered[trueIndex]){
			uptrend[trueIndex] = shortEMAvalue > longEMAvalue; 
			if(entryConditionsMet(entryHi)) enterPosition(trueIndex, entryHi, nVal); 
			return; 
		}
	}

	private boolean exitConditionsMet(int trueIndex, int exitLow){
		if(posEntered[trueIndex])
			return (prices[low] <= stopPrice[trueIndex] || prices[low] <= daysHiLows[exitLow]);
		else return false; 
	}

	private boolean entryConditionsMet(int entryHigh){
		return prices[high] > daysHiLows[entryHigh]; 
	}

	private void exitPosition(int trueIndex, int exitLow){

		posEntered[trueIndex] = false; 

		String action = "";

		if(prices[open] <= daysHiLows[exitLow]){
			exitPrice[trueIndex] = prices[open]; 
			action = "SELL"; 
		}else if(prices[open] <= stopPrice[trueIndex]){
			exitPrice[trueIndex] = prices[open]; 
			action = "STOP";
		}else if(prices[low] <= daysHiLows[exitLow]){
			exitPrice[trueIndex] = daysHiLows[exitLow]; 
			action = "SELL";
		}else if(prices[low] <= stopPrice[trueIndex]){
			exitPrice[trueIndex] = stopPrice[trueIndex]; 
			action = "STOP"; 
		}else{
			exitPrice[trueIndex] = prices[close];
			action = "EOF_SELL";
		}

		exitCapital[trueIndex] = exitPrice[trueIndex] * (double)shares[trueIndex];

		realizedGains[trueIndex] = exitCapital[trueIndex] - entryCapital[trueIndex]; 
		startCapital[trueIndex] = startCapital[trueIndex] + realizedGains[trueIndex]; 

		double percentGain = (exitPrice[trueIndex] / entryPrice[trueIndex] - 1.0) * 100.00; 

		lineBuilder[trueIndex] += (action+"\t"+date+"\t"+exitPrice[trueIndex]+"\t"+
				realizedGains[trueIndex]+"\t"+percentGain+"\t"+startCapital[trueIndex]); 

		ArrayList<String> a = results.get(newKey.toString());
		a.add(lineBuilder[trueIndex]);
		results.put(newKey.toString(), a);
		entryPrice[trueIndex] = 0.0; 
		exitPrice[trueIndex] = 0.0; 
		shares[trueIndex] = 0; 
		stopPrice[trueIndex] = 0.0;
		entryCapital[trueIndex] = 0.0; 
		exitCapital[trueIndex] = 0.0; 
		realizedGains[trueIndex] = 0.0; 
		lineBuilder[trueIndex] = ""; 

	}

	private void enterPosition(int trueIndex, int entryHi, int nVal){
		posEntered[trueIndex] = true; 

		if(prices[open] >= daysHiLows[entryHi]){
			// entry price = open price
			entryPrice[trueIndex] = prices[open]; 
		}else{
			//entry price = new high
			entryPrice[trueIndex] = daysHiLows[entryHi]; 
		}
		shares[trueIndex] = (int)(startCapital[trueIndex] / entryPrice[trueIndex]); 
		stopPrice[trueIndex] = entryPrice[trueIndex] - nVals[nVal];
		entryCapital[trueIndex] = entryPrice[trueIndex] * shares[trueIndex]; 

		lineBuilder[trueIndex] = date+"\t"+startCapital[trueIndex]+"\t"+entryPrice[trueIndex]
				+"\t"+shares[trueIndex]+"\t"+nVals[nVal]+"\t"+stopPrice[trueIndex]+"\t";

	}


}
