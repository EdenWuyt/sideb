import { Component, inject } from '@angular/core';
import { Cart as CartService } from '../../core/services/cart';
import { CartItem } from './cart-item/cart-item';
import { OrderSummary } from '../../shared/components/order-summary/order-summary';

@Component({
  selector: 'app-cart',
  imports: [
    CartItem,
    OrderSummary,
  ],
  templateUrl: './cart.html',
  styleUrl: './cart.css',
})
export class Cart {
  cartService = inject(CartService);
}
