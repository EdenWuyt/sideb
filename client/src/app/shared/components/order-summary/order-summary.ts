import { CurrencyPipe } from '@angular/common';
import { Component, inject } from '@angular/core';
import { MatButton } from '@angular/material/button';
import { MatFormField, MatInput, MatLabel } from '@angular/material/input';
import { RouterLink } from '@angular/router';
import { Cart } from '../../../core/services/cart';
import { Location } from '@angular/common';

@Component({
  selector: 'app-order-summary',
  imports: [
    CurrencyPipe,
    MatButton,
    RouterLink,
    MatFormField,
    MatInput,
    MatLabel,
],
  templateUrl: './order-summary.html',
  styleUrl: './order-summary.css',
})
export class OrderSummary {
  cartService = inject(Cart);
  location = inject(Location);
}
