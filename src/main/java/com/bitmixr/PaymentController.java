package com.bitmixr;

import javassist.NotFoundException;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/")
public class PaymentController extends DefaultExceptionHandler {
	@PersistenceContext
	protected EntityManager entityManager;

	@Autowired
	BitMixrService bitmixrService;

	@RequestMapping(method = RequestMethod.POST)
	@Transactional
	public @ResponseBody
	Payment add(@RequestBody final Payment aPayment) {
		bitmixrService.addPayment(aPayment);
		return aPayment;
	}

	@RequestMapping(value = "/{id}", method = RequestMethod.GET)
	@Transactional
	public @ResponseBody
	Payment show(@PathVariable final String id) throws NotFoundException {
		Payment payment;
		try {
			payment = entityManager.find(Payment.class, id);
		} catch (Exception e) {
			throw new NotFoundException("ID not found.");
		}
		if (!payment.isVisible()) {
			throw new NotFoundException("ID not found.");
		}
		return payment;
	}
}
