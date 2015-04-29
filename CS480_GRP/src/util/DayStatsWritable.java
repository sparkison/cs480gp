package util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DayStatsWritable implements Writable, Comparable<DayStatsWritable>{
    

	private Text ticker;
    
    private Text date;
    
    private DoubleWritable open;
    private DoubleWritable high; 
    private DoubleWritable low; 
    private DoubleWritable close;
    private LongWritable volume; 
    private DoubleWritable adjClose; 
    private DoubleWritable nValue; 
    private LongWritable obv; 
    
    private DoubleWritable sma20; 
    private DoubleWritable BBupper; 
    private DoubleWritable BBlower; 
    
    private DoubleWritable EMA12;
    private DoubleWritable EMA26; 
    private DoubleWritable EMA50; 
    private DoubleWritable EMA75; 
    private DoubleWritable EMA150; 
    private DoubleWritable EMA200; 

    private DoubleWritable twentyHigh; 
    private DoubleWritable thirtyHigh;
    private DoubleWritable fiftyFiveHigh;
    private DoubleWritable twoFiftyHigh; 
    private DoubleWritable tenLow; 
    private DoubleWritable twentyLow; 
    private DoubleWritable thirtyLow;
    
    public DayStatsWritable(Text value) {
        
        String line = value.toString().trim(); 
        if(line.endsWith(",")){
        	line = line.substring(0, line.length()-2);
        }
        String[] parsed = line.trim().split(","); 
        
        this.ticker = new Text(parsed[0].trim()); 
        this.date = new Text(parsed[1].trim()); 
        
        this.open = new DoubleWritable(Double.parseDouble(parsed[2].trim()));
        this.high = new DoubleWritable(Double.parseDouble(parsed[3].trim()));
        this.low = new DoubleWritable(Double.parseDouble(parsed[4].trim())); 
        this.close = new DoubleWritable(Double.parseDouble(parsed[5].trim()));
        this.volume = new LongWritable(Long.parseLong(parsed[6].trim())); 
        this.adjClose = new DoubleWritable(Double.parseDouble(parsed[7].trim())); 
        this.nValue = new DoubleWritable(Double.parseDouble(parsed[8].trim()));; 
        this.obv = new LongWritable(Long.parseLong(parsed[9].trim())); 
        
        this.sma20 = new DoubleWritable(Double.parseDouble(parsed[10].trim())); 
        this.BBupper = new DoubleWritable(Double.parseDouble(parsed[11].trim())); 
        this.BBlower = new DoubleWritable(Double.parseDouble(parsed[12].trim())); 
        
        this.EMA12 = new DoubleWritable(Double.parseDouble(parsed[13].trim()));
        this.EMA26 = new DoubleWritable(Double.parseDouble(parsed[14].trim())); 
        this.EMA50 = new DoubleWritable(Double.parseDouble(parsed[15].trim())); 
        this.EMA75 = new DoubleWritable(Double.parseDouble(parsed[16].trim())); 
        this.EMA150 = new DoubleWritable(Double.parseDouble(parsed[17].trim())); 
        this.EMA200 = new DoubleWritable(Double.parseDouble(parsed[18].trim())); 

        this.twentyHigh = new DoubleWritable(Double.parseDouble(parsed[19].trim())); 
        this.thirtyHigh = new DoubleWritable(Double.parseDouble(parsed[20].trim()));
        this.fiftyFiveHigh = new DoubleWritable(Double.parseDouble(parsed[21].trim()));
        this.twoFiftyHigh = new DoubleWritable(Double.parseDouble(parsed[22].trim())); 
        this.tenLow = new DoubleWritable(Double.parseDouble(parsed[23].trim())); 
        this.twentyLow = new DoubleWritable(Double.parseDouble(parsed[24].trim())); 
        this.thirtyLow = new DoubleWritable(Double.parseDouble(parsed[25].trim())); 
    }
    
    public DayStatsWritable() {
        this.ticker = new Text(); 
        this.date = new Text(); 
        this.open = new DoubleWritable(); 
        this.high = new DoubleWritable();
        this.low = new DoubleWritable();
        this.close = new DoubleWritable();
        this.volume = new LongWritable();
        this.adjClose = new DoubleWritable();
        this.nValue = new DoubleWritable();
        this.obv = new LongWritable();
        
        this.sma20 = new DoubleWritable();
        this.BBupper = new DoubleWritable();
        this.BBlower  = new DoubleWritable();
        
        this.EMA12 = new DoubleWritable();
        this.EMA26 = new DoubleWritable();
        this.EMA50 = new DoubleWritable();
        this.EMA75  = new DoubleWritable();
        this.EMA150  = new DoubleWritable();
        this.EMA200 = new DoubleWritable();

        this.twentyHigh = new DoubleWritable();
        this.thirtyHigh = new DoubleWritable();
        this.fiftyFiveHigh = new DoubleWritable();
        this.twoFiftyHigh = new DoubleWritable();
        this.tenLow = new DoubleWritable();
        this.twentyLow  = new DoubleWritable();
        this.thirtyLow = new DoubleWritable();
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
    	ticker.write(dataOutput);
        date.write(dataOutput);
        open.write(dataOutput);
        high.write(dataOutput);
        low.write(dataOutput);
        close.write(dataOutput);
        volume.write(dataOutput);
        adjClose.write(dataOutput);
        nValue.write(dataOutput);
        obv.write(dataOutput);
        sma20.write(dataOutput);
        BBupper.write(dataOutput);
        BBlower.write(dataOutput);
        EMA12.write(dataOutput);
        EMA26.write(dataOutput);
        EMA50.write(dataOutput);
        EMA75.write(dataOutput);
        EMA150.write(dataOutput);
        EMA200.write(dataOutput);
        twentyHigh.write(dataOutput);
        thirtyHigh.write(dataOutput);
        fiftyFiveHigh.write(dataOutput);
        twoFiftyHigh.write(dataOutput);
        tenLow.write(dataOutput);
        twentyLow.write(dataOutput);
        thirtyLow.write(dataOutput);
    }
    
    @Override
    public void readFields(DataInput dataInput) throws IOException {
    	ticker.readFields(dataInput);
        date.readFields(dataInput);
        open.readFields(dataInput);
        high.readFields(dataInput);
        low.readFields(dataInput);
        close.readFields(dataInput);
        volume.readFields(dataInput);
        adjClose.readFields(dataInput);
        nValue.readFields(dataInput);
        obv.readFields(dataInput);
        sma20.readFields(dataInput);
        BBupper.readFields(dataInput);
        BBlower.readFields(dataInput);
        EMA12.readFields(dataInput);
        EMA26.readFields(dataInput);
        EMA50.readFields(dataInput);
        EMA75.readFields(dataInput);
        EMA150.readFields(dataInput);
        EMA200.readFields(dataInput);
        twentyHigh.readFields(dataInput);
        thirtyHigh.readFields(dataInput);
        fiftyFiveHigh.readFields(dataInput);
        twoFiftyHigh.readFields(dataInput);
        tenLow.readFields(dataInput);
        twentyLow.readFields(dataInput);
        thirtyLow.readFields(dataInput);
    }
    
    public Text getTicker() {return ticker;}
    
    public void setTicker(Text ticker) {this.ticker = ticker;}
    
	public Text getDate() {return date;}

	public void setDate(Text date) {this.date = date;}

	public DoubleWritable getOpen() {return open;}

	public void setOpen(DoubleWritable open) {this.open = open;}

	public DoubleWritable getHigh() {return high;}

	public void setHigh(DoubleWritable high) {this.high = high;}

	public DoubleWritable getLow() {return low;}

	public void setLow(DoubleWritable low) {this.low = low;}

	public DoubleWritable getClose() {return close;}

	public void setClose(DoubleWritable close) {this.close = close;}

	public LongWritable getVolume() {return volume;}

	public void setVolume(LongWritable volume) {this.volume = volume;}

	public DoubleWritable getAdjClose() {return adjClose;}

	public void setAdjClose(DoubleWritable adjClose) {this.adjClose = adjClose;}

	public DoubleWritable getnValue() {return nValue;}

	public void setnValue(DoubleWritable nValue) {this.nValue = nValue;}

	public LongWritable getObv() {return obv;}

	public void setObv(LongWritable obv) {this.obv = obv;}

	public DoubleWritable getSma20() {return sma20;}

	public void setSma20(DoubleWritable sma20) {this.sma20 = sma20;}

	public DoubleWritable getBBupper() {return BBupper;}

	public void setBBupper(DoubleWritable bBupper) {BBupper = bBupper;}

	public DoubleWritable getBBlower() {return BBlower;}

	public void setBBlower(DoubleWritable bBlower) {BBlower = bBlower;}

	public DoubleWritable getEMA12() {return EMA12;}

	public void setEMA12(DoubleWritable eMA12) {EMA12 = eMA12;}

	public DoubleWritable getEMA26() {return EMA26;}

	public void setEMA26(DoubleWritable eMA26) {EMA26 = eMA26;}

	public DoubleWritable getEMA50() {return EMA50;}

	public void setEMA50(DoubleWritable eMA50) {EMA50 = eMA50;}

	public DoubleWritable getEMA75() {return EMA75;}

	public void setEMA75(DoubleWritable eMA75) {EMA75 = eMA75;}

	public DoubleWritable getEMA150() {return EMA150;}

	public void setEMA150(DoubleWritable eMA150) {EMA150 = eMA150;}

	public DoubleWritable getEMA200() {return EMA200;}

	public void setEMA200(DoubleWritable eMA200) {EMA200 = eMA200;}

	public DoubleWritable getTwentyHigh() {return twentyHigh;}

	public void setTwentyHigh(DoubleWritable twentyHigh) {this.twentyHigh = twentyHigh;}

	public DoubleWritable getThirtyHigh() {return thirtyHigh;}

	public void setThirtyHigh(DoubleWritable thirtyHigh) {this.thirtyHigh = thirtyHigh;}

	public DoubleWritable getFiftyFiveHigh() {return fiftyFiveHigh;}

	public void setFiftyFiveHigh(DoubleWritable fiftyFiveHigh) {this.fiftyFiveHigh = fiftyFiveHigh;}

	public DoubleWritable getTwoFiftyHigh() {return twoFiftyHigh;}

	public void setTwoFiftyHigh(DoubleWritable twoFiftyHigh) {this.twoFiftyHigh = twoFiftyHigh;}

	public DoubleWritable getTenLow() {return tenLow;}

	public void setTenLow(DoubleWritable tenLow) {this.tenLow = tenLow;}

	public DoubleWritable getTwentyLow() {return twentyLow;}

	public void setTwentyLow(DoubleWritable twentyLow) {this.twentyLow = twentyLow;}

	public DoubleWritable getThirtyLow() {return thirtyLow;}

	public void setThirtyLow(DoubleWritable thirtyLow) {this.thirtyLow = thirtyLow;}

	@Override
	public int compareTo(DayStatsWritable o) {
		return this.date.compareTo(o.getDate());
	}

	@Override
	public String toString() {
		return "DayStatsWritable [ticker=" + ticker + ", date=" + date
				+ ", open=" + open + ", high=" + high + ", low=" + low
				+ ", close=" + close + ", volume=" + volume + ", adjClose="
				+ adjClose + ", nValue=" + nValue + ", obv=" + obv + ", sma20="
				+ sma20 + ", BBupper=" + BBupper + ", BBlower=" + BBlower
				+ ", EMA12=" + EMA12 + ", EMA26=" + EMA26 + ", EMA50=" + EMA50
				+ ", EMA75=" + EMA75 + ", EMA150=" + EMA150 + ", EMA200="
				+ EMA200 + ", twentyHigh=" + twentyHigh + ", thirtyHigh="
				+ thirtyHigh + ", fiftyFiveHigh=" + fiftyFiveHigh
				+ ", twoFiftyHigh=" + twoFiftyHigh + ", tenLow=" + tenLow
				+ ", twentyLow=" + twentyLow + ", thirtyLow=" + thirtyLow + "]";
	}
    
}
