import { inject, Injectable } from '@angular/core';
import { Cart } from './cart';
import { of } from 'rxjs';

@Injectable({
  providedIn: 'root',
})
export class Init {
  private cartService = inject(Cart);

  init() {
    const cartId = localStorage.getItem('cart_id');
    const cart$ = cartId ? this.cartService.getCart(cartId) : of(null); 
    return cart$
  }
}
