package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class CleanUpWritable implements Writable, Comparable<CleanUpWritable>{
//	
//	valout.set(date+"\t"+action+"\t"+nVals[nVal]+"\t"+entryPrice[posIndex]+"\t"+exitPrice[posIndex]+"\t"+
//			shares[posIndex]+"\t"+stopPrice[posIndex]+"\t"+startCapital[posIndex]+"\t"+entryCapital[posIndex]+"\t"+
//			exitCapital[posIndex]+"\t"+realizedGains[posIndex]);
	
	private Text ticker; 
	private Text date; 
	private Text action; 
	private DoubleWritable nVal; 
	private DoubleWritable entryPrice; 
	private DoubleWritable exitPrice; 
	private IntWritable shares; 
	private DoubleWritable stopPrice; 
	private DoubleWritable startCapital; 
	private DoubleWritable entryCapital; 
	private DoubleWritable exitCapital; 
	private DoubleWritable realizedGain; 
    
    public CleanUpWritable(Text value) {
    	
    	String[] parsed = value.toString().trim().split("\t"); 
    	
    	this.ticker = new Text(parsed[0].trim()); 
    	this.date = new Text(parsed[1].trim());
    	this.action = new Text(parsed[2].trim());
    	this.nVal = new DoubleWritable(Double.parseDouble(parsed[3].trim()));
    	this.entryPrice =  new DoubleWritable(Double.parseDouble(parsed[4].trim()));
    	this.exitPrice =  new DoubleWritable(Double.parseDouble(parsed[5].trim()));
    	this.shares =  new IntWritable(Integer.parseInt(parsed[6].trim()));
    	this.stopPrice =  new DoubleWritable(Double.parseDouble(parsed[7].trim()));
    	this.startCapital =  new DoubleWritable(Double.parseDouble(parsed[8].trim()));
    	this.entryCapital =  new DoubleWritable(Double.parseDouble(parsed[9].trim()));
    	this.exitCapital =  new DoubleWritable(Double.parseDouble(parsed[10].trim()));
    	this.realizedGain =  new DoubleWritable(Double.parseDouble(parsed[11].trim()));
    }
    
    public CleanUpWritable() {
    	this.ticker = new Text();
    	this.date = new Text();
    	this.action = new Text();
    	this.nVal = new DoubleWritable();
    	this.entryPrice = new DoubleWritable();
    	this.exitPrice = new DoubleWritable();
    	this.shares = new IntWritable();
    	this.stopPrice = new DoubleWritable();
    	this.startCapital = new DoubleWritable();
    	this.entryCapital = new DoubleWritable();
    	this.exitCapital = new DoubleWritable();
    	this.realizedGain = new DoubleWritable(); 
    }

	@Override
	public void readFields(DataInput datainput) throws IOException {
    	this.ticker.readFields(datainput);
    	this.date.readFields(datainput);
    	this.action.readFields(datainput);
    	this.nVal.readFields(datainput);
    	this.entryPrice.readFields(datainput);
    	this.exitPrice.readFields(datainput);
    	this.shares.readFields(datainput);
    	this.stopPrice.readFields(datainput);
    	this.startCapital.readFields(datainput);
    	this.entryCapital.readFields(datainput);
    	this.exitCapital.readFields(datainput);
    	this.realizedGain.readFields(datainput);
	}

	@Override
	public void write(DataOutput dataoutput) throws IOException {
    	this.ticker.write(dataoutput);
    	this.date.write(dataoutput);
    	this.action.write(dataoutput);
    	this.nVal.write(dataoutput);
    	this.entryPrice.write(dataoutput);
    	this.exitPrice.write(dataoutput);
    	this.shares.write(dataoutput);
    	this.stopPrice.write(dataoutput);
    	this.startCapital.write(dataoutput);
    	this.entryCapital.write(dataoutput);
    	this.exitCapital.write(dataoutput);
    	this.realizedGain.write(dataoutput);
	}
	
	public Text getTicker() {
		return ticker;
	}

	public void setTicker(Text ticker) {
		this.ticker = ticker;
	}

	public Text getDate() {
		return date;
	}

	public void setDate(Text date) {
		this.date = date;
	}

	public Text getAction() {
		return action;
	}

	public void setAction(Text action) {
		this.action = action;
	}

	public DoubleWritable getnVal() {
		return nVal;
	}

	public void setnVal(DoubleWritable nVal) {
		this.nVal = nVal;
	}

	public DoubleWritable getEntryPrice() {
		return entryPrice;
	}

	public void setEntryPrice(DoubleWritable entryPrice) {
		this.entryPrice = entryPrice;
	}

	public DoubleWritable getExitPrice() {
		return exitPrice;
	}

	public void setExitPrice(DoubleWritable exitPrice) {
		this.exitPrice = exitPrice;
	}

	public IntWritable getShares() {
		return shares;
	}

	public void setShares(IntWritable shares) {
		this.shares = shares;
	}

	public DoubleWritable getStopPrice() {
		return stopPrice;
	}

	public void setStopPrice(DoubleWritable stopPrice) {
		this.stopPrice = stopPrice;
	}

	public DoubleWritable getStartCapital() {
		return startCapital;
	}

	public void setStartCapital(DoubleWritable startCapital) {
		this.startCapital = startCapital;
	}

	public DoubleWritable getEntryCapital() {
		return entryCapital;
	}

	public void setEntryCapital(DoubleWritable entryCapital) {
		this.entryCapital = entryCapital;
	}

	public DoubleWritable getExitCapital() {
		return exitCapital;
	}

	public void setExitCapital(DoubleWritable exitCapital) {
		this.exitCapital = exitCapital;
	}

	public DoubleWritable getRealizedGain() {
		return realizedGain;
	}

	public void setRealizedGain(DoubleWritable realizedGain) {
		this.realizedGain = realizedGain;
	}

	@Override
	public int compareTo(CleanUpWritable o) {
		return this.date.compareTo(o.getDate());
	}
	
	public String toString(){
		String stringer = "";
		stringer += date.toString()+"\t"; 
		stringer += action.toString()+"\t"; 
		stringer += nVal.get()+"\t"; 
		stringer += entryPrice.get()+"\t"; 
		stringer += exitPrice.get()+"\t"; 
		stringer += shares.get()+"\t"; 
		stringer += stopPrice.get()+"\t"; 
		stringer += startCapital.get()+"\t"; 
		stringer += entryCapital.get()+"\t"; 
		stringer += exitCapital.get()+"\t"; 
		stringer += realizedGain.get(); 
		return stringer;
	}

}