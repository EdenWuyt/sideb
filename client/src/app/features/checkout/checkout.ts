import { Component, inject, OnDestroy, OnInit, signal } from '@angular/core';
import { OrderSummary } from "../../shared/components/order-summary/order-summary";
import { MatStepper, MatStepperModule } from "@angular/material/stepper";
import { Router, RouterLink } from '@angular/router';
import { MatButton } from '@angular/material/button';
import { Stripe } from '../../core/services/stripe';
import { StripeAddressElement } from '@stripe/stripe-js/dist/stripe-js/elements/address';
import { Snackbar } from '../../core/services/snackbar';
import { MatCheckboxModule, MatCheckboxChange } from '@angular/material/checkbox';
import { StepperSelectionEvent } from '@angular/cdk/stepper';
import { Address } from '../../shared/models/user';
import { firstValueFrom } from 'rxjs/internal/firstValueFrom';
import { Account } from '../../core/services/account';
import { CheckoutDelivery } from './checkout-delivery/checkout-delivery';
import { StripePaymentElement } from '@stripe/stripe-js/dist/stripe-js/elements/payment';
import { CheckoutReview } from './checkout-review/checkout-review';
import { ConfirmationToken } from '@stripe/stripe-js';
import { Cart } from '../../core/services/cart';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { OrderToCreate, ShippingAddress } from '../../shared/models/order';
import { Order } from '../../core/services/order';

@Component({
  selector: 'app-checkout',
  imports: [
    OrderSummary,
    MatStepperModule,
    MatButton,
    RouterLink,
    MatCheckboxModule,
    CheckoutDelivery,
    CheckoutReview,
    MatProgressSpinnerModule
  ],
  templateUrl: './checkout.html',
  styleUrl: './checkout.css',
})
export class Checkout implements OnInit, OnDestroy {
  private router = inject(Router);
  private stripeService = inject(Stripe);
  private accountService = inject(Account);
  private cartService = inject(Cart);
  private orderService = inject(Order);
  private snackbar = inject(Snackbar);
  addressElement?: StripeAddressElement;
  paymentElement?: StripePaymentElement;
  saveAddress: boolean = false;
  completionStatus = signal<{address: boolean, delivery: boolean, card: boolean}>({address: false, delivery: false, card: false});
  confirmationToken = signal<ConfirmationToken | null>(null);
  confirmPaymentLoading = signal<boolean>(false);
  orderConfirmed = signal<boolean>(false);

  async ngOnInit() {
    try {
      this.addressElement = await this.stripeService.createAddressElement();
      this.addressElement.mount('#address-element');
      this.addressElement.on('change', (event) => {
        this.completionStatus.update((status) => ({...status, address: event.complete}));
      });
      this.paymentElement = await this.stripeService.createPaymentElement();
      this.paymentElement.mount('#payment-element');
      this.paymentElement.on('change', (event) => {
        this.completionStatus.update((status) => ({...status, card: event.complete}));
      });
    } catch (error: any) {
      this.snackbar.error(error.message);
    }
  }

  ngOnDestroy() {
    this.stripeService.disposeElements();
  }

  onSaveAddressCheckboxChange(event: MatCheckboxChange) {
    this.saveAddress = event.checked;
  }

  handleDeliveryChange(deliveryComplete: boolean) {
    this.completionStatus.update((status) => ({...status, delivery: deliveryComplete}));
  }

  async getConfirmationToken() {
    try {
      if (Object.values(this.completionStatus()).every(status => status === true)) {
        const result = await this.stripeService.createConfirmationToken();
        if (result.error) throw new Error(result.error.message);
        this.confirmationToken.set(result.confirmationToken);
      }
    } catch (error: any) {
      this.snackbar.error(error.message);
    }
  }

  async onStepChange(event: StepperSelectionEvent) {
    if (event.selectedIndex === 1) {
      if (this.saveAddress) {
        const address = await this.getAdreessFromStripeElement() as Address;
        address && firstValueFrom(this.accountService.updateAddress(address));
      }
    }
    if (event.selectedIndex === 2) {
      await firstValueFrom(this.stripeService.createOrUpdatePaymentIntent());
    }
    if (event.selectedIndex === 3) {
      await this.getConfirmationToken();
      this.orderConfirmed.set(true);
    }
  }

  async confirmPayment(stepper: MatStepper) {
    this.confirmPaymentLoading.set(true); 
    try {
      const confirmationToken = this.confirmationToken();
      if (!confirmationToken) throw new Error('Confirmation token not found');
      const result = await this.stripeService.confirmPayment(confirmationToken);
      if (result.paymentIntent?.status === 'succeeded') {
        const order = await this.createOrderModel();
        const orderResult = await firstValueFrom(this.orderService.createOrder(order));
        if (orderResult) {
          this.orderService.orderCompleted.set(true);
          this.cartService.deleteCart();
          this.cartService.selectedDeliveryMethod.set(null);
          this.router.navigateByUrl('/checkout/success');
        } else {
          throw new Error('Order creation failed');
        }
      } else if (result.error) {
        throw new Error(result.error.message);
      } else {
        throw new Error('Something went wrong during payment confirmation');
      }
    } catch (error: any) {
      this.snackbar.error(error.message || 'Payment failed');
      stepper.previous();
    } finally {
      this.confirmPaymentLoading.set(false);
    }
  }

  private async createOrderModel(): Promise<OrderToCreate> {
    const cart = this.cartService.cart();
    const shippingAddress = await this.getAdreessFromStripeElement() as ShippingAddress;
    const card = this.confirmationToken()?.payment_method_preview.card;
    if (!cart?.id || !cart.deliveryMethodId || !shippingAddress || !card) throw new Error('Problem creating order');
    return {
      cartId: cart.id,
      deliveryMethodId: cart.deliveryMethodId,
      shippingAddress: shippingAddress,
      paymentSummary: {
        last4Digits: +card.last4,
        brand: card.brand,
        expMonth: card.exp_month,
        expYear: card.exp_year
      },
      discount: this.cartService.totals()?.discount || 0
    };
  }

  private async getAdreessFromStripeElement(): Promise<Address | ShippingAddress | null> {
    const result = await this.addressElement?.getValue();
    const address = result?.value.address;

    if (address) {
      return {
        name: result.value.name,
        line1: address.line1,
        line2: address.line2 || undefined,
        city: address.city,
        state: address.state,
        postalCode: address.postal_code,
        country: address.country,
      };
    } else return null;
  }
}
