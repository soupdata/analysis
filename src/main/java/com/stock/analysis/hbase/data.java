package com.stock.analysis.hbase;

public class data {
	
private String item_id;
private String stock_code;
private double item_vaule1;
private double item_vaule2;
private double item_vaule3;


public data(){
	
}


public data(String item_id, String stock_code, double item_vaule1,
		double item_vaule2, double item_vaule3) {
	super();
	this.item_id = item_id;
	this.stock_code = stock_code;
	this.item_vaule1 = item_vaule1;
	this.item_vaule2 = item_vaule2;
	this.item_vaule3 = item_vaule3;
}



public String getItem_id() {
	return item_id;
}
public void setItem_id(String item_id) {
	this.item_id = item_id;
}
public String getStock_code() {
	return stock_code;
}
public void setStock_code(String stock_code) {
	this.stock_code = stock_code;
}
public double getItem_vaule1() {
	return item_vaule1;
}
public void setItem_vaule1(double item_vaule1) {
	this.item_vaule1 = item_vaule1;
}
public double getItem_vaule2() {
	return item_vaule2;
}
public void setItem_vaule2(double item_vaule2) {
	this.item_vaule2 = item_vaule2;
}
public double getItem_vaule3() {
	return item_vaule3;
}
public void setItem_vaule3(double item_vaule3) {
	this.item_vaule3 = item_vaule3;
}


@Override
public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((item_id == null) ? 0 : item_id.hashCode());
	long temp;
	temp = Double.doubleToLongBits(item_vaule1);
	result = prime * result + (int) (temp ^ (temp >>> 32));
	temp = Double.doubleToLongBits(item_vaule2);
	result = prime * result + (int) (temp ^ (temp >>> 32));
	temp = Double.doubleToLongBits(item_vaule3);
	result = prime * result + (int) (temp ^ (temp >>> 32));
	result = prime * result
			+ ((stock_code == null) ? 0 : stock_code.hashCode());
	return result;
}


@Override
public boolean equals(Object obj) {
	if (this == obj)
		return true;
	if (obj == null)
		return false;
	if (getClass() != obj.getClass())
		return false;
	data other = (data) obj;
	if (item_id == null) {
		if (other.item_id != null)
			return false;
	} else if (!item_id.equals(other.item_id))
		return false;
	if (Double.doubleToLongBits(item_vaule1) != Double
			.doubleToLongBits(other.item_vaule1))
		return false;
	if (Double.doubleToLongBits(item_vaule2) != Double
			.doubleToLongBits(other.item_vaule2))
		return false;
	if (Double.doubleToLongBits(item_vaule3) != Double
			.doubleToLongBits(other.item_vaule3))
		return false;
	if (stock_code == null) {
		if (other.stock_code != null)
			return false;
	} else if (!stock_code.equals(other.stock_code))
		return false;
	return true;
}


@Override
public String toString() {
	return "data [item_id=" + item_id + ", stock_code=" + stock_code
			+ ", item_vaule1=" + item_vaule1 + ", item_vaule2=" + item_vaule2
			+ ", item_vaule3=" + item_vaule3 + "]";
}

}
