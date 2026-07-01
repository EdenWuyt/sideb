import { Component, inject, OnInit, signal } from '@angular/core';
import { Order as OrderService } from '../../core/services/order';
import { Order as OrderType } from '../../shared/models/order';
import { RouterLink } from '@angular/router';
import { CurrencyPipe, DatePipe } from '@angular/common';

@Component({
  selector: 'app-order',
  imports: [
    RouterLink,
    DatePipe,
    CurrencyPipe,
  ],
  templateUrl: './order.html',
  styleUrl: './order.css',
})
export class Order implements OnInit {
  private orderService = inject(OrderService);
  orders = signal<OrderType[]>([]);

  ngOnInit() {
    this.orderService.getOrdersForUser().subscribe({
      next: (orders) => this.orders.set(orders),
      error: (error) => console.error(error),
    });
  }
}
