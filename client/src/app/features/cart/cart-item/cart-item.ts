import { Component, inject, input } from '@angular/core';
import { RouterLink } from '@angular/router';
import { CartItem as CartItemEntity } from '../../../shared/models/cart';
import { MatButton, MatIconButton } from '@angular/material/button';
import { MatIcon } from '@angular/material/icon';
import { CurrencyPipe } from '@angular/common';
import { Cart } from '../../../core/services/cart';
import { Snackbar } from '../../../core/services/snackbar';

@Component({
  selector: 'app-cart-item',
  imports: [
    RouterLink,
    MatButton,
    MatIcon,
    MatIconButton,
    CurrencyPipe,
],
  templateUrl: './cart-item.html',
  styleUrl: './cart-item.css',
})
export class CartItem {
  item = input.required<CartItemEntity>();
  cartService = inject(Cart);
  snackbar = inject(Snackbar);

  incrementQuantity() {
    this.cartService.addItemToCart(this.item());
  }

  decrementQuantity() {
    this.cartService.removeItemFromCart(this.item().productId);
  }

  removeItemFromCart() {
    this.cartService.removeItemFromCart(this.item().productId, this.item().quantity);
  }
}
