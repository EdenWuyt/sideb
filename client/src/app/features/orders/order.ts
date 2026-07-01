import { Component, inject, OnInit, signal } from '@angular/core';
import { Order as OrderService } from '../../core/services/order';
import { Order as OrderType } from '../../shared/models/order';
import { Router, RouterLink } from '@angular/router';
import { CurrencyPipe, DatePipe } from '@angular/common';
import { EmptyState } from '../../shared/components/empty-state/empty-state';

@Component({
  selector: 'app-order',
  imports: [
    RouterLink,
    DatePipe,
    CurrencyPipe,
    EmptyState
  ],
  templateUrl: './order.html',
  styleUrl: './order.css',
})
export class Order implements OnInit {
  private orderService = inject(OrderService);
  private router = inject(Router);
  orders = signal<OrderType[]>([]);

  ngOnInit() {
    this.orderService.getOrdersForUser().subscribe({
      next: (orders) => this.orders.set(orders),
      error: (error) => console.error(error),
    });
  }

  continueShopping() {
    this.router.navigateByUrl('/shop');
  }
}
