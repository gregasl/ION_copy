/*
 * ION Trading U.K. Limited supplies this software code is for testing purposes 
 * only. The scope of this software code is exclusively limited to the 
 * demonstration of the structure of an application using the ION(tm) Common 
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior 
 * of any deployed application using this software code.
 * This software code has not been thoroughly tested under all conditions.
 * ION, therefore, cannot guarantee or imply reliability, serviceability, or 
 * function of this software.
 * Any use of this software outside of this defined scope is the sole 
 * responsibility of the user.
 *
 * ION Trading ltd (2005)
*/

package com.iontrading.samples.simplePublisher2;

public class MyPrice {
  
  private String ID;
  private Double ASK;
  private Double BID;
  private Double QTY;

  public String getID() {
    return ID;
  }

  public void setID(String ID) {
    this.ID = ID;
  }

  public Double getASK() {
    return ASK;
  }

  public void setASK(Double ASK) {
    this.ASK = ASK;
  }

  public Double getBID() {
    return BID;
  }

  public void setBID(Double BID) {
    this.BID = BID;
  }

  public Double getQTY() {
    return QTY;
  }

  public void setQTY(Double QTY) {
    this.QTY = QTY;
  }

}
