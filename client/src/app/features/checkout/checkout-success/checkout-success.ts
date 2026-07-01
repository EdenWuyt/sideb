import { Component, inject, OnDestroy } from '@angular/core';
import { MatButton } from '@angular/material/button';
import { RouterLink } from '@angular/router';
import { Signalr } from '../../../core/services/signalr';
import { PaymentMethodPipe } from '../../../shared/pipes/payment-method-pipe';
import { CurrencyPipe, DatePipe } from '@angular/common';
import { AddressPipe } from '../../../shared/pipes/address-pipe';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { Order } from '../../../core/services/order';

@Component({
  selector: 'app-checkout-success',
  imports: [
    RouterLink,
    MatButton,
    MatProgressSpinnerModule,
    DatePipe,
    AddressPipe,
    CurrencyPipe,
    PaymentMethodPipe,
  ],
  templateUrl: './checkout-success.html',
  styleUrl: './checkout-success.css',
})
export class CheckoutSuccess implements OnDestroy {
  signalrService = inject(Signalr);
  private orderService = inject(Order);

  ngOnDestroy() {
    this.orderService.orderCompleted.set(false);
    this.signalrService.orderSignal.set(null);
  }
}
