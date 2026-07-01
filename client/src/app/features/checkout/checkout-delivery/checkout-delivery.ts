import { Component, inject, OnInit, output } from '@angular/core';
import { Checkout } from '../../../core/services/checkout';
import { MatRadioModule } from '@angular/material/radio';
import { CurrencyPipe } from '@angular/common';
import { Cart } from '../../../core/services/cart';
import { DeliveryMethod } from '../../../shared/models/deliveryMethod';

@Component({
  selector: 'app-checkout-delivery',
  imports: [
    MatRadioModule,
    CurrencyPipe,
  ],
  templateUrl: './checkout-delivery.html',
  styleUrl: './checkout-delivery.css',
})
export class CheckoutDelivery implements OnInit {
  checkoutService = inject(Checkout);
  cartService = inject(Cart);
  deliveryComplete = output<boolean>();

  ngOnInit(): void {
    this.checkoutService.getDeliveryMethods().subscribe({
      next: (deliveryMethods) => {
        if (this.cartService.cart()?.deliveryMethodId){
          const method = deliveryMethods.find(m => m.id === this.cartService.cart()?.deliveryMethodId);
          if (method) {
            this.cartService.selectedDeliveryMethod.set(method);
            this.deliveryComplete.emit(true);
          }
        }
      }
    });
  }

  updateSelectedDeliveryMethod(method: DeliveryMethod) {
    this.cartService.selectedDeliveryMethod.set(method);
    const cart = this.cartService.cart();
    if (cart) {
      cart.deliveryMethodId = method.id;
      this.cartService.setCart(cart);
      this.deliveryComplete.emit(true);
    }
  }
}
