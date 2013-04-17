package com.bitmixr;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.bitcoin.core.ECKey;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.Wallet;
import com.google.bitcoin.core.WalletEventListener;

public class WalletActor implements WalletEventListener {

	public Wallet wallet;
	public String paymentId;
	public Set<SeenTransaction> seenTransactions = new HashSet<>();
	public ReentrantReadWriteLock seenTransactionLock = new ReentrantReadWriteLock();
	
	public WalletActor(Wallet aWallet, String aPaymentId) {
		wallet = aWallet;
		paymentId = aPaymentId;
		wallet.addEventListener(this);
	}

	@Override
	public void onCoinsReceived(Wallet wallet, Transaction tx, BigInteger prevBalance, BigInteger newBalance) {
		SeenTransaction seenTransaction = new SeenTransaction();
		seenTransaction.setAmount(tx.getValueSentToMe(wallet));
		seenTransaction.setTransactionHash(tx.getHashAsString());
		tx.getConfidence().addEventListener(seenTransaction);
		seenTransactionLock.writeLock().lock();
		seenTransactions.add(seenTransaction);
		seenTransactionLock.writeLock().unlock();
	}

	@Override
	public void onCoinsSent(Wallet wallet, Transaction tx, BigInteger prevBalance, BigInteger newBalance) {
	}

	@Override
	public void onKeyAdded(ECKey key) {
	}

	@Override
	public void onReorganize(Wallet wallet) {
	}

	@Override
	public void onTransactionConfidenceChanged(Wallet wallet, Transaction tx) {
	}

	@Override
	public void onWalletChanged(Wallet wallet) {
	}
}
