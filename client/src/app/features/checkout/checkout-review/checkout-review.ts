import { Component, inject, Input } from '@angular/core';
import { Cart } from '../../../core/services/cart';
import { CurrencyPipe } from '@angular/common';
import { ConfirmationToken } from '@stripe/stripe-js';
import { AddressPipe } from '../../../shared/pipes/address-pipe';
import { PaymentMethodPipe } from '../../../shared/pipes/payment-method-pipe';

@Component({
  selector: 'app-checkout-review',
  imports: [
    CurrencyPipe,
    AddressPipe,
    PaymentMethodPipe
],
  templateUrl: './checkout-review.html',
  styleUrl: './checkout-review.css',
})
export class CheckoutReview {
  @Input() confirmationToken: ConfirmationToken | null = null;
  cartService = inject(Cart);
}
