import { Component, inject, signal } from '@angular/core';
import { Order as OrderService } from '../../../core/services/order';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { Order as OrderType } from '../../../shared/models/order';
import { MatCardModule } from '@angular/material/card';
import { MatButton } from '@angular/material/button';
import { CurrencyPipe, DatePipe } from '@angular/common';
import { AddressPipe } from "../../../shared/pipes/address-pipe";
import { PaymentMethodPipe } from "../../../shared/pipes/payment-method-pipe";
import { MatIcon } from '@angular/material/icon';

@Component({
  selector: 'app-order-details',
  imports: [
    MatCardModule,
    MatButton,
    MatIcon,
    RouterLink,
    CurrencyPipe,
    DatePipe,
    AddressPipe,
    PaymentMethodPipe
],
  templateUrl: './order-details.html',
  styleUrl: './order-details.css',
})
export class OrderDetails {
  private orderService = inject(OrderService);
  private activatedRoute = inject(ActivatedRoute);
  order = signal<OrderType | null>(null);

  ngOnInit() {
    this.loadOrder();
  }

  loadOrder() {
    const orderId = this.activatedRoute.snapshot.paramMap.get('id');
    if (!orderId) return;
    this.orderService.getOrderDetails(+orderId).subscribe({
      next: (order) => this.order.set(order),
      error: (error) => console.error(error),
    });
  }
}
