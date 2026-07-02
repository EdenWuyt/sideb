import { CurrencyPipe } from '@angular/common';
import { Component, inject, input, OnInit, signal } from '@angular/core';
import { MatButton } from '@angular/material/button';
import { MatFormField, MatInput, MatLabel } from '@angular/material/input';
import { RouterLink } from '@angular/router';
import { Cart } from '../../../core/services/cart';
import { Location } from '@angular/common';
import { firstValueFrom } from 'rxjs/internal/firstValueFrom';
import { FormsModule } from '@angular/forms';
import { MatIcon } from '@angular/material/icon';
import { Stripe } from '../../../core/services/stripe';

@Component({
  selector: 'app-order-summary',
  imports: [
    CurrencyPipe,
    MatButton,
    RouterLink,
    MatFormField,
    MatInput,
    MatLabel,
    FormsModule,
    MatIcon,
],
  templateUrl: './order-summary.html',
  styleUrl: './order-summary.css',
})
export class OrderSummary {
  cartService = inject(Cart);
  private stripeService = inject(Stripe);
  location = inject(Location);
  couponCode?: string;
  orderConfirmed = input<boolean>(false);

  applyCoupon() {
    if (!this.couponCode) return;
    this.cartService.applyDiscount(this.couponCode).subscribe({
        next: async coupon => {
          const cart = this.cartService.cart();
          if (cart) {
            cart.coupon = coupon;
            await firstValueFrom(this.cartService.setCart(cart));
            this.couponCode = undefined;
            if (this.location.path() === '/checkout') {
              await firstValueFrom(this.stripeService.createOrUpdatePaymentIntent());
            }
          }
        },
        error: (error) => console.error(error)
      });
  }

  async removeCoupon() {
    const cart = this.cartService.cart();
    if (!cart) return;
    if (cart.coupon) cart.coupon = undefined;
    await firstValueFrom(this.cartService.setCart(cart));
    if (this.location.path() === '/checkout') {
      await firstValueFrom(this.stripeService.createOrUpdatePaymentIntent());
    }
  }
}
