package com.bitmixr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cpn.apiomatic.rest.RestCommand;
import com.google.bitcoin.core.Address;
import com.google.bitcoin.core.AddressFormatException;
import com.google.bitcoin.core.BlockChain;
import com.google.bitcoin.core.ECKey;
import com.google.bitcoin.core.NetworkParameters;
import com.google.bitcoin.core.PeerGroup;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.Utils;
import com.google.bitcoin.core.VerificationException;
import com.google.bitcoin.core.Wallet;
import com.google.bitcoin.core.Wallet.SendRequest;
import com.google.bitcoin.core.WrongNetworkException;
import com.google.bitcoin.discovery.DnsDiscovery;
import com.google.bitcoin.discovery.SeedPeers;
import com.google.bitcoin.store.BlockStore;
import com.google.bitcoin.store.BlockStoreException;
import com.google.bitcoin.store.MemoryFullPrunedBlockStore;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

@Service
public class BitMixrService {

	@PersistenceContext
	protected EntityManager entityManager;

	BlockStore blockStore;
	BlockChain chain;
	PeerGroup peerGroup;
	NetworkParameters params = NetworkParameters.prodNet();

	Map<String, WalletActor> actors = new HashMap<>();
	ReentrantReadWriteLock actorsLock = new ReentrantReadWriteLock();
	ReentrantLock lock = new ReentrantLock();

	AtomicBoolean started = new AtomicBoolean(false);

	List<NetworkReceive> networkReceives = new ArrayList<>();
	ReentrantLock networkReceivesLock = new ReentrantLock();

	ExecutorService executor = Executors.newCachedThreadPool();

	public BlockStore getBlockStore() {
		return blockStore;
	}

	public void setBlockStore(BlockStore blockStore) {
		this.blockStore = blockStore;
	}

	public BlockChain getChain() {
		return chain;
	}

	public void setChain(BlockChain chain) {
		this.chain = chain;
	}

	public PeerGroup getPeerGroup() {
		return peerGroup;
	}

	public void setPeerGroup(PeerGroup peerGroup) {
		this.peerGroup = peerGroup;
	}

	public NetworkParameters getParams() {
		return params;
	}

	public void setParams(NetworkParameters params) {
		this.params = params;
	}

	public Map<String, WalletActor> getActors() {
		return actors;
	}

	public void setActors(Map<String, WalletActor> actors) {
		this.actors = actors;
	}

	public ReentrantReadWriteLock getActorsLock() {
		return actorsLock;
	}

	public void setActorsLock(ReentrantReadWriteLock actorsLock) {
		this.actorsLock = actorsLock;
	}

	public ReentrantLock getLock() {
		return lock;
	}

	public void setLock(ReentrantLock lock) {
		this.lock = lock;
	}

	public AtomicBoolean getStarted() {
		return started;
	}

	public void setStarted(AtomicBoolean started) {
		this.started = started;
	}

	public List<NetworkReceive> getNetworkRecieves() {
		return networkReceives;
	}

	public void setNetworkRecieves(List<NetworkReceive> networkRecieves) {
		this.networkReceives = networkRecieves;
	}

	public ReentrantLock getNetworkRecieveslock() {
		return networkReceivesLock;
	}

	public void setNetworkRecieveslock(ReentrantLock networkRecieveslock) {
		this.networkReceivesLock = networkRecieveslock;
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public void setExecutor(ExecutorService executor) {
		this.executor = executor;
	}

	public void init() throws BlockStoreException {
		blockStore = new MemoryFullPrunedBlockStore(params, 500);
		chain = new BlockChain(params, blockStore);
		peerGroup = new PeerGroup(params, chain);
		peerGroup.setUserAgent("BitMixr", "1.0");
		peerGroup.addPeerDiscovery(new SeedPeers(params));
		peerGroup.addPeerDiscovery(new DnsDiscovery(params));
		peerGroup.setFastCatchupTimeSecs(new GregorianCalendar(2013, 4, 1).getTimeInMillis() / 1000);
	}

	@Transactional
	public void load() {
		actorsLock.writeLock().lock();
		List<Payment> payments = entityManager.createQuery("from Payment", Payment.class).getResultList();
		for (Payment p : payments) {
			Wallet wallet = new Wallet(params);
			ECKey key = p.getECKey();
			wallet.addKey(key);
			peerGroup.addWallet(wallet);
			chain.addWallet(wallet);
			WalletActor actor = new WalletActor(wallet, p.getId());
			actorsLock.writeLock().lock();
			actors.put(p.getId(), actor);
			actorsLock.writeLock().unlock();
		}
		actorsLock.writeLock().unlock();

	}

	public void startUp() {
		peerGroup.startAndWait();
		peerGroup.waitForPeers(2);
		peerGroup.downloadBlockChain();
	}

	@Transactional
	public void addPayment(Payment aPayment) {
		if (!started.get()) {
			throw new IllegalStateException("The block chain hasn't downloaded yet. Give it just a few more minutes and try again.");
		}
		aPayment.setSentAmount(BigInteger.ZERO);
		aPayment.setRecievedAmount(BigInteger.ZERO);
		aPayment.setSpentAmount(BigInteger.ZERO);
		aPayment.setReceivedTip(BigInteger.ZERO);
		aPayment.setSpentTip(BigInteger.ZERO);
		aPayment.setCreatedOn(new Date());
		aPayment.setUpdatedOn(new Date());
		aPayment.setPaidOn(null);
		
		ECKey key = new ECKey();
		aPayment.setECKey(key);
		aPayment.setSourceAddress(key.toAddress(params).toString());
		Wallet wallet = new Wallet(params);
		wallet.addKey(key);
		WalletActor actor = new WalletActor(wallet, aPayment.getId());
		actorsLock.writeLock().lock();
		actors.put(aPayment.getId(), actor);
		actorsLock.writeLock().unlock();
		chain.addWallet(wallet);
		peerGroup.addWallet(wallet);
		entityManager.persist(aPayment);
	}

	@Scheduled(fixedDelay = 20000)
	@Transactional
	public void checkSeenTransactions() throws BlockStoreException {
		lock.lock();
		if (!started.get()) {
			init();
			load();
			startUp();
			started.set(true);
		}
		actorsLock.readLock().lock();
		for (WalletActor actor : actors.values()) {
			actor.seenTransactionLock.writeLock().lock();
			for (Iterator<SeenTransaction> i = actor.seenTransactions.iterator(); i.hasNext();) {
				SeenTransaction seenTransaction = i.next();
				if (seenTransaction.getDirty().get()) {
					Payment payment = entityManager.find(Payment.class, actor.paymentId);
					try {
						entityManager.createQuery("from SeenTransaction where transactionHash = ? and payment = ?", SeenTransaction.class).setParameter(1, seenTransaction.getTransactionHash()).setParameter(2, payment).getSingleResult();
					} catch (Exception e) {

						seenTransaction.setPayment(payment);
						entityManager.persist(seenTransaction);
						payment.getSeenTransactions().add(seenTransaction);
						BigInteger originalAmount = seenTransaction.getAmount();
						BigInteger tip = new BigDecimal(originalAmount).multiply(new BigDecimal(2)).divide(new BigDecimal(100)).toBigInteger();
						payment.setRecievedAmount(payment.getRecievedAmount().add(originalAmount.subtract(tip)));
						payment.setReceivedTip(payment.getReceivedTip().add(tip));
						payment.setUpdatedOn(new Date());
						entityManager.merge(payment);
					}
					i.remove();
				}
			}
			actor.seenTransactionLock.writeLock().unlock();
		}
		actorsLock.readLock().unlock();
		lock.unlock();
	}

	@Transactional
	@Scheduled(fixedDelay = 20000)
	public void payOut() {
		lock.lock();
		Calendar sixtySecondsAgo = new GregorianCalendar();
		sixtySecondsAgo.add(Calendar.SECOND, -60);
		List<Payment> paymentsNeedingSending = entityManager.createQuery("from Payment where (paidOn is null or paidOn < :sixtySecondsAgo) and sentAmount < recievedAmount and recievedAmount - sentAmount > :minimumAmount order by createdOn", Payment.class).setParameter("minimumAmount", Utils.CENT)
				.setParameter("sixtySecondsAgo", sixtySecondsAgo.getTime()).getResultList();
		for (final Payment payee : paymentsNeedingSending) {
			List<Payment> paymentsAvailableForSending = entityManager.createQuery("from Payment where (paidOn is null or paidOn < :sixtySecondsAgo) and recievedAmount - spentAmount > :minimumAmount and id != :excludedId", Payment.class).setParameter("minimumAmount", Utils.CENT).setParameter("excludedId", payee.getId()).setParameter("sixtySecondsAgo", sixtySecondsAgo).getResultList();
			if (paymentsAvailableForSending.size() < 1) {
				continue;
			}
			Collections.shuffle(paymentsAvailableForSending);
			Payment tempPayer = null;
			for (Payment p : paymentsAvailableForSending) {
				if (p.getPaidOn() != null && p.getPaidOn().getTime() > System.currentTimeMillis() - 60000) {
					continue;
				}
				tempPayer = p;
				break;
			}
			if (tempPayer == null) {
				continue;
			}
			final Payment payer = tempPayer;
			BigInteger amountAvailableForSending = payer.getRecievedAmount().subtract(payer.getSpentAmount());
			BigInteger amountNeedingToBeSent = payee.getRecievedAmount().subtract(payee.getSentAmount());
			final BigInteger amountToSend;

			if (amountAvailableForSending.compareTo(amountNeedingToBeSent) == 0) {
				amountToSend = amountAvailableForSending;
			} else if (amountAvailableForSending.compareTo(amountNeedingToBeSent) > 0) {
				amountToSend = amountNeedingToBeSent;
			} else {
				amountToSend = amountAvailableForSending;
			}

			// Now send the coins back!
			final Address destination;
			try {
				destination = new Address(params, payee.getDestinationAddress());
			} catch (AddressFormatException e1) {
				throw new RuntimeException(e1.getMessage(), e1);
			}

			Transaction tx = new Transaction(params);
			tx.addOutput(amountToSend, destination);
			BigInteger tip = BigInteger.ZERO;
			if (payer.getReceivedTip().subtract(payer.getSpentTip()).compareTo(BigInteger.ZERO) > 0) {
				AddressResponse tipAddress = new RestCommand<String, AddressResponse>("https://blockchain.info/merchant/e3625e3b-81ff-830e-9fcd-59c64bd4ade9/new_address?password=wookiechew&second_password=rockandroll!!!&label=" + payer.getId(), AddressResponse.class).get();
				tip = payer.getReceivedTip().subtract(payer.getSpentTip());
				try {
					tx.addOutput(tip, new Address(params, tipAddress.getAddress()));
				} catch (WrongNetworkException e) {
					throw new RuntimeException(e.getMessage(), e);
				} catch (AddressFormatException e) {
					throw new RuntimeException(e.getMessage(), e);
				}
			}
			final BigInteger realTip = tip;
			SendRequest request = Wallet.SendRequest.forTx(tx);
			Wallet wallet = actors.get(payer.getId()).wallet;
			if (!wallet.completeTx(request))
				return; // Insufficient funds.
			// Ensure these funds won't be spent
			// again.
			try {
				wallet.commitTx(request.tx);
			} catch (VerificationException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
			// wallet.saveToFile(...);
			// A proposed transaction is now sitting
			// in request.tx - send it in the
			// background.

			Futures.addCallback(peerGroup.broadcastTransaction(request.tx), new FutureCallback<Transaction>() {
				@Override
				public void onFailure(Throwable t) {
					t.printStackTrace();
				}

				@Override
				public void onSuccess(Transaction result) {
					networkReceivesLock.lock();
					networkReceives.add(new NetworkReceive(payer.getId(), payee.getId(), amountToSend, realTip));
					networkReceivesLock.unlock();
				}
			}, executor);
			payer.setPaidOn(new Date());
			payee.setPaidOn(new Date());
			entityManager.merge(payer);
			entityManager.merge(payee);
		}
		lock.unlock();
	}

	@Scheduled(fixedDelay = 2000)
	@Transactional
	public void commitSends() {
		lock.lock();
		networkReceivesLock.lock();
		for (NetworkReceive send : networkReceives) {
			Payment payer = entityManager.find(Payment.class, send.getPayerId());
			Payment payee = entityManager.find(Payment.class, send.getPayeeId());
			payer.setSpentAmount(payer.getSpentAmount().add(send.getAmountSpent()));
			payer.setSpentTip(payer.getSpentTip().add(send.getTip()));
			payee.setSentAmount(payee.getSentAmount().add(send.getAmountSpent()));
			payer.setPaidOn(null);
			payee.setPaidOn(null);
			payer = entityManager.merge(payer);
			payee = entityManager.merge(payee);
		}
		networkReceives.clear();
		networkReceivesLock.unlock();
		lock.unlock();
	}

	@Transactional
	@Scheduled(fixedDelay = 60000)
	public void expirePayments() {
		lock.lock();
		Date now = new Date();
		Calendar twoWeeksAgo = new GregorianCalendar();
		twoWeeksAgo.add(Calendar.DAY_OF_YEAR, -14);
		BigInteger minimumAmount = Utils.CENT;
		List<Payment> payments = entityManager.createQuery("from Payment where (updatedOn < :twoWeeksAgo or (expiresOn is not null and expiresOn < :now)) or (totalToSend > 0 and sentAmount >= totalToSend and recievedAmount - spentAmount < :minimumAmount)", Payment.class)
				.setParameter("twoWeeksAgo", twoWeeksAgo.getTime()).setParameter("now", now).setParameter("minimumAmount", minimumAmount).getResultList();
		for (Payment p : payments) {
			ExpiredECKey key = new ExpiredECKey(p);
			entityManager.persist(key);
			entityManager.remove(p);
		}
		lock.unlock();
	}

}
