import { Component, inject } from '@angular/core';
import { Cart as CartService } from '../../core/services/cart';
import { CartItem } from './cart-item/cart-item';
import { OrderSummary } from '../../shared/components/order-summary/order-summary';
import { EmptyState } from '../../shared/components/empty-state/empty-state';
import { Router } from '@angular/router';

@Component({
  selector: 'app-cart',
  imports: [
    CartItem,
    OrderSummary,
    EmptyState
  ],
  templateUrl: './cart.html',
  styleUrl: './cart.css',
})
export class Cart {
  private router = inject(Router);
  cartService = inject(CartService);

  continueShopping() {
    this.router.navigateByUrl('/shop');
  }
}
