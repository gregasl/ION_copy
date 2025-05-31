package com.iontrading.samples.advanced.orderManagement;

/**
 * Interface defining the core order management operations.
 * Implementations are responsible for:
 * 1. Tracking order lifecycle (especially when they can no longer trade)
 * 2. Receiving market best price updates
 * 3. Creating new orders on the market
 */
public interface IOrderManager {

	/**
	 * Notifies that an order is "dead" (cannot generate further trades).
	 * This can happen when an order:
	 * - Is completely filled
	 * - Expires
	 * - Is canceled
	 * - Is rejected
	 * 
	 * @param order The market order that is now dead
	 */
	public void orderDead(MarketOrder order);

	/**
	 * Creates and puts an order on the market with the specified parameters.
	 * 
	 * @param instrId The instrument identifier of the venue ID
	 * @param instrId The instrument identifier of the trader ID for the venue
	 * @param instrId The instrument identifier of the NATIVE venue
	 * @param verb The order direction ("Buy" or "Sell")
	 * @param qty The quantity of the order
	 * @param price The price of the order
	 * @param type The order type (e.g., "Limit", "Market")
	 * @param tif Time in force (e.g., "Day", "IOC", "FOK", "FAS", "FAK")
	 * @return The created MarketOrder object
	 */
	public MarketOrder addOrder(String MarketSource, String TraderId, String instrId, String verb, double qty,
			double price, String type, String tif);

	/**
	 * Notifies of a best price update for an instrument.
	 * Implementation may decide to take action (like aggressing) based on this update.
	 * 
	 * @param best The updated best price information
	 */
	public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG);

	/**
	 * Returns an identifier unique among clients that can put orders on the market.
	 * Used to distinguish orders sent by this component from orders sent by other components.
	 * 
	 * @return The unique application identifier
	 */
	public String getApplicationId();

		/**
	 * Maps an order ID to a request ID for lookup purposes
	 * 
	 * @param orderId The order ID from the market
	 * @param reqId The internal request ID
	 */
	void mapOrderIdToReqId(String orderId, int reqId);
}